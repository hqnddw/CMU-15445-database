/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once


#include <memory>
#include <unordered_map>
#include <mutex>
#include "buffer/replacer.h"

using namespace std;
namespace cmudb {

    template <typename T> class LRUReplacer : public Replacer<T> {
        //采用双向链表
        struct Node {
            Node() {};
            Node(T val) : val(val) {};
            T val;
            //使用智能指针封装链表节点
            shared_ptr<Node> prev;
            shared_ptr<Node> next;
        };
    public:
        // do not change public interface
        LRUReplacer();

        ~LRUReplacer();

        void Insert(const T &value);

        bool Victim(T &value);

        bool Erase(const T &value);

        size_t Size();
        // add your member variables here
    private:
        shared_ptr<Node> head;
        shared_ptr<Node> tail;
        // 使用unordered_map存储key和链表节点的映射
        unordered_map<T,shared_ptr<Node>> map;
        mutable mutex latch;
    };

} // namespace cmudb
