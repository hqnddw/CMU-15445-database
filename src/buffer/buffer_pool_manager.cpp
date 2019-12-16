#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/**
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::BufferPoolManager(size_t pool_size,
                                         DiskManager *disk_manager,
                                         LogManager *log_manager)
            : pool_size_(pool_size),
              disk_manager_(disk_manager),
              log_manager_(log_manager) {
        //为缓冲池申请一段连续的内存
        pages_ = new Page[pool_size_];
        //页表采用之前实现的可扩展的哈希表
        page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
        replacer_ = new LRUReplacer<Page *>;
        // free_list存储空闲页面
        free_list_ = new std::list<Page *>;

        // put all the pages into free list
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_->push_back(&pages_[i]);
        }
    }

/**
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
    BufferPoolManager::~BufferPoolManager() {
        delete[] pages_;
        delete page_table_;
        delete replacer_;
        delete free_list_;
    }

/**
 * fetch page_id page
 * This function must mark the Page as pinned and remove its entry from
 * LRUReplacer before it is returned to the caller.
 */
/**
 *1.先从页表中寻找，
    1.1 找到的话立即返回
    1.2 找不到的话从free_list或者lru placer中获取page
  2.如果获取到的页面是dirty，将页面上的内容写回磁盘
  3.删除页表中旧的page_id和对应的页面，把新的page_id和对应的页面插入到页表中
  4.把磁盘上要求page_id的页面中的内容写到获取到的页面中
*/
    Page *BufferPoolManager::FetchPage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
        Page *tar = nullptr;
        // 1 在页表中查找
        if (page_table_->Find(page_id, tar)) {
            tar->pin_count_++;
            replacer_->Erase(tar);
            return tar;
        }
        // 从free_list或lru placer中获取空闲页面
        tar = GetVictimPage();
        if (tar == nullptr) return tar;
        if (tar->is_dirty_) {
            if (ENABLE_LOGGING && log_manager_->GetPersistentLSN() < tar->GetLSN())
                log_manager_->Flush(true);
            //2 将页面中的内容写回到磁盘
            disk_manager_->WritePage(tar->GetPageId(), tar->data_);
        }
        // 3
        page_table_->Remove(tar->GetPageId());
        page_table_->Insert(page_id, tar);
        // 4
        disk_manager_->ReadPage(page_id, tar->data_);
        tar->pin_count_ = 1;
        tar->is_dirty_ = false;
        tar->page_id_ = page_id;

        return tar;
    }

    //Implementation of unpin page
/**
 * 1. 如果pin_count<=0, 返回false
 * 2. 如果pin_count>0, pin_count自减，把is_dirty设为给定的标志
 *    2.1 自减后为0，把这个页面放到lru replacer中，返回true
*/
    bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
        lock_guard<mutex> lck(latch_);
        Page *tar = nullptr;
        page_table_->Find(page_id, tar);
        if (tar == nullptr) {
            return false;
        }
        tar->is_dirty_ |= is_dirty;

        if (tar->GetPinCount() <= 0) {
            assert(false);
            return false;
        };
        if (--tar->pin_count_ == 0) {
            replacer_->Insert(tar);
        }
        return true;
    }

/**
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
/**
 * 1. 找到页面后，如果页面is_dirty标志为true，把页面上的内容写回到磁盘上，返回true
 * 2. 没有找到页面，返回false
*/
    bool BufferPoolManager::FlushPage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
        Page *tar = nullptr;
        page_table_->Find(page_id, tar);
        if (tar == nullptr || tar->page_id_ == INVALID_PAGE_ID) {
            return false;
        }
        if (tar->is_dirty_) {
            disk_manager_->WritePage(page_id, tar->GetData());
            tar->is_dirty_ = false;
        }

        return true;
    }

/**
 * User should call this method for deleting a page.
 */
/**
 * 1.在页表中寻找该页面
 *   1.1 如果没找到，则从磁盘上把该页面删除掉
 *   1.2 如果页面存在但pin_count不为0，返回false，如果pin_count为0，进入下一步
 * 2.从replacer中删除该页面，从页表中删除该条目，把is_dirty标志设为fasle
 * 清空页面的内容，并把页面的id设为无效值，最后把页面加入到free_list中
*/
    bool BufferPoolManager::DeletePage(page_id_t page_id) {
        lock_guard<mutex> lck(latch_);
        Page *tar = nullptr;
        page_table_->Find(page_id, tar);
        if (tar != nullptr) {
            if (tar->GetPinCount() > 0) {
                return false;
            }
            replacer_->Erase(tar);
            page_table_->Remove(page_id);
            tar->is_dirty_ = false;
            tar->ResetMemory();
            tar->page_id_ = INVALID_PAGE_ID;
            free_list_->push_back(tar);
        }
        disk_manager_->DeallocatePage(page_id);
        return true;
    }

//User should call this method if needs to create a new page.
/**
 * 1.从free_list或replacer中获取一个victim page
 *    1.1 所有页面都在使用，返回空指针
 *    1.2 获取到了，进入下一步
 * 2.如果获取到的页面是脏页面，则把页面上的内容写回磁盘
 * 3.更新页表中条目信息
 * 4.清空page上的内容，并更新page自身的id，is_dirty标志，pin_count设为1
*/
    Page *BufferPoolManager::NewPage(page_id_t &page_id) {
        lock_guard<mutex> lck(latch_);
        Page *tar = nullptr;
        tar = GetVictimPage();
        if (tar == nullptr) {
            return tar;
        }

        page_id = disk_manager_->AllocatePage();
        if (tar->is_dirty_) {
            if (ENABLE_LOGGING && log_manager_->GetPersistentLSN() < tar->GetLSN())
                log_manager_->Flush(true);
            disk_manager_->WritePage(tar->GetPageId(), tar->data_);
        }
        page_table_->Remove(tar->GetPageId());
        page_table_->Insert(page_id, tar);

        tar->page_id_ = page_id;
        tar->ResetMemory();
        tar->is_dirty_ = false;
        tar->pin_count_ = 1;

        return tar;
    }

    Page *BufferPoolManager::GetVictimPage() {
        Page *tar = nullptr;
        if (free_list_->empty()) {
            if (replacer_->Size() == 0) {
                //所有的页面都在使用
                return nullptr;
            }
            replacer_->Victim(tar);
        } else {
            tar = free_list_->front();
            free_list_->pop_front();
            assert(tar->GetPageId() == INVALID_PAGE_ID);
        }
        if (tar != nullptr) {
            //确保页面不在使用中
            assert(tar->GetPinCount() == 0);
        }
        return tar;
    }

// DEBUG
    bool BufferPoolManager::CheckAllUnpined() {
        bool res = true;
        for (size_t i = 1; i < pool_size_; i++) {
            if (pages_[i].pin_count_ != 0) {
                res = false;
                std::cout << "page " << pages_[i].page_id_
                          << " pin count:" << pages_[i].pin_count_ << endl;
            }
        }
        return res;
    }

}  // namespace cmudb
