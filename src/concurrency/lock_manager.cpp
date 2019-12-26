/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
using namespace std;

namespace cmudb {

    bool LockManager::LockShared(Transaction *txn, const RID &rid) {
        return lockTemplate(txn, rid, LockMode::SHARED);
    }

    bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
        return lockTemplate(txn, rid, LockMode::EXCLUSIVE);
    }

    bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
        return lockTemplate(txn, rid, LockMode::UPGRADING);
    }
/**
 * 在LOCK TEMPLATE 中，大致分为4个模块
 * 第一个模块是找到对应的TX LIST并且获得锁
 * 第二个模块是针对LOCK UPGRADING，因为需要抹掉原来的读锁，才能升级为写锁。
 * 第三个模块是判断是否可以GRANT。
 * 第四个模块就是往TX LIST里插入，同时阻塞或者拿锁成功就往TXN
 * 里面放入对应的RID记录
 */
    bool LockManager::lockTemplate(Transaction *txn, const RID &rid,
                                   LockMode mode) {
        // step 1
        //事务在缩减阶段，不能加锁
        if (txn->GetState() != TransactionState::GROWING) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        unique_lock<mutex> tableLatch(mutex_);
        TxList &txList = lockTable_[rid];
        unique_lock<mutex> txListLatch(txList.mutex_);
        tableLatch.unlock();

        if (mode == LockMode::UPGRADING) {  // step 2
            if (txList.hasUpgrading_) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            //找到第一个list中第一个和txn id相同的事务
            auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                              [txn](const TxItem &item) {
                                  return item.tid_ == txn->GetTransactionId();
                              });
            if (it == txList.locks_.end() || it->mode_ != LockMode::SHARED ||
                !it->granted_) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
            txList.locks_.erase(it);
            assert(txn->GetSharedLockSet()->erase(rid) == 1);
        }
        // step 3
        bool canGrant = txList.checkCanGrant(mode);
        // WAIT-DIE policy
        //当事务Ti申请的数据项被Tj持有，仅当Ti的时间戳小于Tj的（Ti比Tj老）时，
        //允许Ti等待，否则Ti回滚（死亡）
        if (!canGrant && txList.locks_.back().tid_ < txn->GetTransactionId()) {
            txn->SetState(TransactionState::ABORTED);
            return false;
        }
        // step 4
        txList.insert(txn, rid, mode, canGrant, &txListLatch);
        return true;
    }

/**
 * 1.在UNLOCK里，首先要区分是否是S2PL，是的话就要求只能在COMMIT和ABORT的时候才可以释放锁。
 * 2.随后定位到要删除的元素的TXLIST，从里面抹除，从TRANSACTIONS的LOCK集合里抹除对应的RID。
 * 3.然后判断是否TXLIST EMPTY，抹除对应的KEY。
 * 4.最后判断是否可以GRANT锁给其他的TX。
 */
    bool LockManager::Unlock(Transaction *txn, const RID &rid) {
        if (strict_2PL_) {  // step1
            if (txn->GetState() != TransactionState::COMMITTED &&
                txn->GetState() != TransactionState::ABORTED) {
                txn->SetState(TransactionState::ABORTED);
                return false;
            }
        } else if (txn->GetState() == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
        }
        unique_lock<mutex> tableLatch(mutex_);
        TxList &txList = lockTable_[rid];
        unique_lock<mutex> txListLatch(txList.mutex_);
        // step 2
        auto it = find_if(txList.locks_.begin(), txList.locks_.end(),
                          [txn](const TxItem &item) {
                              return item.tid_ == txn->GetTransactionId();
                          });
        assert(it != txList.locks_.end());
        auto lockSet = it->mode_ == LockMode::SHARED ? txn->GetSharedLockSet()
                                                     : txn->GetExclusiveLockSet();
        //set::erase返回：
        //   1.（输入的参数为迭代器时）后随最后被移除的元素的迭代器
        //   2.（输入的参数为key时）被移除的元素数
        assert(lockSet->erase(rid) == 1);
        txList.locks_.erase(it);
        //step 3
        //如果后面没有事务需要加锁，就把数据项从锁表中删除掉
        if (txList.locks_.empty()) {
            lockTable_.erase(rid);
            return true;
        }
        tableLatch.unlock();
        // step 4
        for (auto &tx : txList.locks_) {
            if (tx.granted_) break;
            tx.Grant();  // grant blocking one
            if (tx.mode_ == LockMode::SHARED) {
                continue;
            }
            if (tx.mode_ == LockMode::UPGRADING) {
                txList.hasUpgrading_ = false;
                tx.mode_ = LockMode::EXCLUSIVE;
            }
            break;
        }
        return true;
    }

}  // namespace cmudb
