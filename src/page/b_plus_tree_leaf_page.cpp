/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <include/page/b_plus_tree_internal_page.h>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
        SetPageType(IndexPageType::LEAF_PAGE);
        SetSize(0);
        assert(sizeof(BPlusTreeLeafPage) == 28);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        SetNextPageId(INVALID_PAGE_ID);
        // 减一是为了预留一个空间当遇到插入满的情况时，简化split的实现
        SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage)) / sizeof(MappingType) - 1);
    }

/**
 * Helper methods to set/get next page id
 */
    INDEX_TEMPLATE_ARGUMENTS
    page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
        return next_page_id_;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
/**
 * 找到第一个符合array[i].first>=key条件的index值
*/
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
            const KeyType &key, const KeyComparator &comparator) const {
        assert(GetSize() >= 0);
        int st = 0, ed = GetSize() - 1;
        while (st <= ed) { //find the last key in array <= input
            int mid = (ed - st) / 2 + st;
            if (comparator(array[mid].first, key) >= 0) ed = mid - 1;
            else st = mid + 1;
        }
        return ed + 1;
    }

/**
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
        assert(index >= 0 && index < GetSize());
        return array[index].first;
    }

/**
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
    INDEX_TEMPLATE_ARGUMENTS
    const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
        assert(index >= 0 && index < GetSize());
        return array[index];
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * 将键和值对插入按键排序的叶子页中，返回插入后叶页面的size值
 * 1. 计算要插入key的index
 * 2. 将数组中index后位置的值都向后移动一位
 * 3. 在index位置插入给定的键值对
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                           const ValueType &value,
                                           const KeyComparator &comparator) {
        //1.
        int idx = KeyIndex(key, comparator); //first larger than key
        assert(idx >= 0);
        IncreaseSize(1);
        int curSize = GetSize();
        //2.
        for (int i = curSize - 1; i > idx; i--) {
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        //3.
        array[idx].first = key;
        array[idx].second = value;
        return curSize;
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/**
 * 如果插入元素后，当前页面的size大于maxSize，
 * 将当前页面的一半元素移动到一个给定的页面recipient中
 * 1. 将当前页面的后半部分元素移动到recipient页面中
 * 2. 将recipient页面插入到当前页面后边
 * 3. 更改当前页面和recipient页面的size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
            BPlusTreeLeafPage *recipient,
            __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
        assert(recipient != nullptr);
        int total = GetMaxSize() + 1;
        assert(GetSize() == total);
        //1.
        int copyIdx = (total) / 2;//7 is 4,5,6,7; 8 is 4,5,6,7,8
        for (int i = copyIdx; i < total; i++) {
            recipient->array[i - copyIdx].first = array[i].first;
            recipient->array[i - copyIdx].second = array[i].second;
        }
        //2.
        recipient->SetNextPageId(GetNextPageId());
        SetNextPageId(recipient->GetPageId());
        //3.
        SetSize(copyIdx);
        recipient->SetSize(total - copyIdx);

    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/**
 * 对于给定的key，检查它是否在叶子页面中存在
 *    1. 如果它存在，将其对应的值存储value中并返回true。
 *    2. 如果key不存在，则返回false
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                            const KeyComparator &comparator) const {
        int idx = KeyIndex(key, comparator);
        if (idx < GetSize() && comparator(array[idx].first, key) == 0) {
            value = array[idx].second;
            return true;
        }
        return false;
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * 首先查找整个leaf page看key是否存在
 *    1. 如果key存在，求出key所在位置的index，并把index+1到数组末尾的元素
 *       向前移动一位，把key所在的键值对覆盖掉，返回删除后page的size大小
 *    2. 如果key不存在，返回size大小
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
            const KeyType &key, const KeyComparator &comparator) {
        int firIdxLargerEqualThanKey = KeyIndex(key, comparator);
        if (firIdxLargerEqualThanKey >= GetSize() || comparator(key, KeyAt(firIdxLargerEqualThanKey)) != 0) {
            return GetSize();
        }
        int tarIdx = firIdxLargerEqualThanKey;
        // void *memmove(void *dst, const void *src, size_t count);
        memmove(array + tarIdx, array + tarIdx + 1,
                static_cast<size_t>((GetSize() - tarIdx - 1) * sizeof(MappingType)));
        IncreaseSize(-1);
        return GetSize();
    }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/**
 * 如果删除元素后，当前页的size小于minSize，那么把当前页的所有元素移动到recipient页面中，
 * 更新recipient的size，并把当前页从leaf page的链表中删除掉
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                               int, BufferPoolManager *) {
        assert(recipient != nullptr);

        int startIdx = recipient->GetSize();//7 is 4,5,6,7; 8 is 4,5,6,7,8
        for (int i = 0; i < GetSize(); i++) {
            recipient->array[startIdx + i].first = array[i].first;
            recipient->array[startIdx + i].second = array[i].second;
        }
        recipient->SetNextPageId(GetNextPageId());
        recipient->IncreaseSize(GetSize());
        SetSize(0);

    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/**
 * 如果当前页面的size小于minSize，且右兄弟页面的size大于minSize时，
 * 从右兄弟页面借一个元素，即把右兄弟页面的第一个元素移动到当前页面的末尾
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
            BPlusTreeLeafPage *recipient,
            BufferPoolManager *buffer_pool_manager) {
        MappingType pair = GetItem(0);
        IncreaseSize(-1);
        memmove(array, array + 1, static_cast<size_t>(GetSize() * sizeof(MappingType)));
        recipient->CopyLastFrom(pair);
        //把当前页和recipient共同的父节点的key替换为当前页新的首元素的key
        Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
        assert(GetSize() + 1 <= GetMaxSize());
        array[GetSize()] = item;
        IncreaseSize(1);
    }
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
            BPlusTreeLeafPage *recipient, int parentIndex,
            BufferPoolManager *buffer_pool_manager) {
        MappingType pair = GetItem(GetSize() - 1);
        IncreaseSize(-1);
        recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
            const MappingType &item, int parentIndex,
            BufferPoolManager *buffer_pool_manager) {
        assert(GetSize() + 1 < GetMaxSize());
        memmove(array + 1, array, GetSize() * sizeof(MappingType));
        IncreaseSize(1);
        array[0] = item;

        Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parentIndex, array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
    INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
        if (GetSize() == 0) {
            return "";
        }
        std::ostringstream stream;
        if (verbose) {
            stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
                   << "]<" << GetSize() << "> ";
        }
        int entry = 0;
        int end = GetSize();
        bool first = true;

        while (entry < end) {
            if (first) {
                first = false;
            } else {
                stream << " ";
            }
            stream << std::dec << array[entry].first;
            if (verbose) {
                stream << "(" << array[entry].second << ")";
            }
            ++entry;
        }
        return stream.str();
    }

    template
    class BPlusTreeLeafPage<GenericKey<4>, RID,
            GenericComparator<4>>;

    template
    class BPlusTreeLeafPage<GenericKey<8>, RID,
            GenericComparator<8>>;

    template
    class BPlusTreeLeafPage<GenericKey<16>, RID,
            GenericComparator<16>>;

    template
    class BPlusTreeLeafPage<GenericKey<32>, RID,
            GenericComparator<32>>;

    template
    class BPlusTreeLeafPage<GenericKey<64>, RID,
            GenericComparator<64>>;
} // namespace cmudb
