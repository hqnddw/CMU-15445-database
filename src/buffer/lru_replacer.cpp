/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

    template <typename T> LRUReplacer<T>::LRUReplacer() {
        head = make_shared<Node>();
        tail = make_shared<Node>();
        //采用虚拟的头尾节点
        head->next = tail;
        tail->prev = head;
    }

    template <typename T> LRUReplacer<T>::~LRUReplacer() {
        //消除前向指针
        while( head ) {
            shared_ptr<Node> tmp = head->next;
            head->next = nullptr;
            head = tmp;
        }
        //消除后向指针
        while (tail) {
            shared_ptr<Node> tmp = tail->prev;
            tail->prev = nullptr;
            tail = tmp;
        }
    }

/*
 * Insert value into LRU
 */
// 首先判断这个value是否在链表中，如果存在把这个节点从链表中提出来
// 如果不在则新建一个节点，最后把提出的节点或新建的节点插入到链表头部
    template <typename T> void LRUReplacer<T>::Insert(const T &value) {
        lock_guard<mutex> lck(latch);
        shared_ptr<Node> cur;
        // 提出已存在的节点
        if (map.find(value) != map.end()) {
            cur = map[value];
            shared_ptr<Node> prev = cur->prev;
            shared_ptr<Node> succ = cur->next;
            prev->next = succ;
            succ->prev = prev;
        } else {
            cur = make_shared<Node>(value);
        }
        // 插入节点
        shared_ptr<Node> fir = head->next;
        cur->next = fir;
        fir->prev = cur;
        cur->prev = head;
        head->next = cur;
        map[value] = cur;
        return;
    }

/* If LRU is non-empty, pop the tail member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
//弹出链表末尾的节点，
// 分三步：1.删除链表中的节点 2.删除map中value和节点的映射关系 3.把victim节点的值保存在value中
    template <typename T> bool LRUReplacer<T>::Victim(T &value) {
        lock_guard<mutex> lck(latch);
        if (map.empty()) {
            return false;
        }
        shared_ptr<Node> last = tail->prev;
        tail->prev = last->prev;
        last->prev->next = tail;
        value = last->val;
        map.erase(last->val);
        return true;
    }

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
//删除链表中指定value的节点
//分两步：1.删除链表中的节点 2.删除map中value和节点的映射关系
    template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
        lock_guard<mutex> lck(latch);
        if (map.find(value) != map.end()) {
            shared_ptr<Node> cur = map[value];
            cur->prev->next = cur->next;
            cur->next->prev = cur->prev;
        }
        return map.erase(value);
    }

    template <typename T> size_t LRUReplacer<T>::Size() {
        lock_guard<mutex> lck(latch);
        return map.size();
    }

    template class LRUReplacer<Page *>;
// test only
    template class LRUReplacer<int>;

} // namespace cmudb
