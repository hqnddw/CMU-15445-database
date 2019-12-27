/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/**
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                              page_id_t parent_id) {
        SetPageType(IndexPageType::INTERNAL_PAGE);
        SetSize(0);
        SetPageId(page_id);
        SetParentPageId(parent_id);
        // 数组中第一个键是无效的并且parent page中的成员变量占一定的空间
        SetMaxSize(
                (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / sizeof(MappingType) - 1);
    }
/**
 * 给定index，获取key
 */
    INDEX_TEMPLATE_ARGUMENTS
    KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
        assert(index >= 0 && index < GetSize());
        return array[index].first;
    }
/**
 * 将index处的key设为给定的key
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
        assert(index >= 0 && index < GetSize());
        array[index].first = key;
    }

/**
 * 获取值为value的index，如果不存在，返回-1
 */
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
        for (int i = 0; i < GetSize(); i++) {
            if (value != ValueAt(i)) continue;
            return i;
        }
        return -1;
    }

/**
 * 获取给定index处的value
 */
    INDEX_TEMPLATE_ARGUMENTS
    ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
        assert(index >= 0 && index < GetSize());
        return array[index].second;
    }

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/**
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
    INDEX_TEMPLATE_ARGUMENTS
    ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
            const KeyType &key, const KeyComparator &comparator) const {
        assert(GetSize() > 1);
        //使用二分查找
        int st = 1, ed = GetSize() - 1;
        while (st <= ed) {  //找到数组中小于等于输入key的最后面的值
            int mid = (ed - st) / 2 + st;
            if (comparator(array[mid].first, key) <= 0)
                st = mid + 1;
            else
                ed = mid - 1;
        }
        //最后执行的一次if判断中，array[mid]>key,所以需要减1
        return array[st - 1].second;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * Populate new root page with old_value + new_key & new_value.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
/**
 * 当插入造成从叶页面一直到根页面溢出时，应该创建一个新的根页面并填充其新的根页面
 */
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
            const ValueType &old_value, const KeyType &new_key,
            const ValueType &new_value) {
        // internal page 的第一个key是无效的
        array[0].second = old_value; //左页面的id
        array[1].first = new_key;
        array[1].second = new_value; // 分裂出来的右页面的id

        SetSize(2);
    }
/**
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
/**
 * 在所给value的后面插入一个新的<key, value>对
 * 1. 先找到value所在的index
 * 2. 将index之后的元素，从后向前逐个向后移动一位
 * 3. 在index+1的位置插入新的键值对，并返回插入后page中键值对的个数
*/
    INDEX_TEMPLATE_ARGUMENTS
    int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
            const ValueType &old_value, const KeyType &new_key,
            const ValueType &new_value) {
        int idx = ValueIndex(old_value) + 1;
        assert(idx > 0);
        IncreaseSize(1);
        int curSize = GetSize();
        for (int i = curSize - 1; i > idx; i--) {
            array[i].first = array[i - 1].first;
            array[i].second = array[i - 1].second;
        }
        array[idx].first = new_key;
        array[idx].second = new_value;
        return curSize;
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/**
 * Remove half of key & value pairs from this page to "recipient" page
 */
/**
 * 如果页面的size大于maxSize，当前页面需要向右分裂出一个新的页面，
 * 并把当前页面的一半元素转移到新的页面中，并把移到新页面中元素所指向的子页面的
 * parent_id改成新页面的id
*/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
            BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
        assert(recipient != nullptr);
        int total = GetMaxSize() + 1;
        assert(GetSize() == total);
        int copyIdx = (total) / 2;  // max:4 x,1,2,3,4 -> 2,3,4
        page_id_t recipPageId = recipient->GetPageId();
        for (int i = copyIdx; i < total; i++) {
            recipient->array[i - copyIdx].first = array[i].first;
            recipient->array[i - copyIdx].second = array[i].second;
            // update children's parent page
            auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
            BPlusTreePage *childTreePage =
                    reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
            childTreePage->SetParentPageId(recipPageId);
            //使用完后，一定要记得将页面取消固定
            buffer_pool_manager->UnpinPage(array[i].second, true);
        }
        //改变当前页面和新页面的size
        SetSize(copyIdx);
        recipient->SetSize(total - copyIdx);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
            MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * 删除指定index的键值对，从后先前移动数组，将index处的元素覆盖掉，并将当前页面的size减一
*/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
        assert(index >= 0 && index < GetSize());
        for (int i = index + 1; i < GetSize(); i++) {
            array[i - 1] = array[i];
        }
        IncreaseSize(-1);
    }

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
    INDEX_TEMPLATE_ARGUMENTS
    ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
        ValueType ret = ValueAt(0);
        IncreaseSize(-1);
        assert(GetSize() == 0);
        return ret;
    }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/**
 * 如果当前页面的size小于minSize时，将当前页面和兄弟页面进行合并
*/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
            BPlusTreeInternalPage *recipient, int index_in_parent,
            BufferPoolManager *buffer_pool_manager) {
        int start = recipient->GetSize();
        page_id_t recipPageId = recipient->GetPageId();
        // first find parent
        Page *page = buffer_pool_manager->FetchPage(GetParentPageId());
        assert(page != nullptr);
        BPlusTreeInternalPage *parent =
                reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());

        // the separation key from parent
        //把父页面的要移动的节点先放在要删除页面（即右页面）的第一个key的位置，因为这个位置原先的key
        //是无效的，再把这个节点移动到兄弟页面
        SetKeyAt(0, parent->KeyAt(index_in_parent));
        buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
        //把当前page的节点全部移到左边的兄弟page中
        for (int i = 0; i < GetSize(); ++i) {
            recipient->array[start + i].first = array[i].first;
            recipient->array[start + i].second = array[i].second;
            // 更新移动元素所指向子页面的parent_id
            auto childRawPage = buffer_pool_manager->FetchPage(array[i].second);
            BPlusTreePage *childTreePage =
                    reinterpret_cast<BPlusTreePage *>(childRawPage->GetData());
            childTreePage->SetParentPageId(recipPageId);
            buffer_pool_manager->UnpinPage(array[i].second, true);
        }
        // 更新合并后页面和当前页面的size
        recipient->SetSize(start + GetSize());
        assert(recipient->GetSize() <= GetMaxSize());
        SetSize(0);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
            MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/**
 * 如果当前页面的size小于minSize，且右兄弟页面的size大于minSize时，
 * 从右兄弟页面借一个元素，即把右兄弟页面的第一个元素移动到当前页面的末尾
*/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
            BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
        //todo:第一个key不是无效的吗
        MappingType pair{KeyAt(0), ValueAt(0)};
        IncreaseSize(-1);
        memmove(array, array + 1,
                static_cast<size_t>(GetSize() * sizeof(MappingType)));
        recipient->CopyLastFrom(pair, buffer_pool_manager);
        // 更新移动元素所指向子页面的parent_id
        page_id_t childPageId = pair.second;
        Page *page = buffer_pool_manager->FetchPage(childPageId);
        assert(page != nullptr);
        BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child->SetParentPageId(recipient->GetPageId());
        assert(child->GetParentPageId() == recipient->GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
        // update relevant key & value pair in its parent page.
        page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parent->ValueIndex(GetPageId()), array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
            const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
        assert(GetSize() + 1 <= GetMaxSize());
        array[GetSize()] = pair;
        IncreaseSize(1);
    }


/**
 * 如果当前页面的size小于minSize，且左兄弟页面的size大于minSize时，
 * 从左兄弟页面借一个元素，即把左兄弟页面的最后一个元素移动到当前页面的头部
*/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
            BPlusTreeInternalPage *recipient, int parent_index,
            BufferPoolManager *buffer_pool_manager) {
        MappingType pair{KeyAt(GetSize() - 1), ValueAt(GetSize() - 1)};
        IncreaseSize(-1);
        recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
    }

    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
            const MappingType &pair, int parent_index,
            BufferPoolManager *buffer_pool_manager) {
        assert(GetSize() + 1 < GetMaxSize());
        memmove(array + 1, array, GetSize() * sizeof(MappingType));
        IncreaseSize(1);
        array[0] = pair;
        page_id_t childPageId = pair.second;
        Page *page = buffer_pool_manager->FetchPage(childPageId);
        assert(page != nullptr);
        BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
        child->SetParentPageId(GetPageId());
        assert(child->GetParentPageId() == GetPageId());
        buffer_pool_manager->UnpinPage(child->GetPageId(), true);
        page = buffer_pool_manager->FetchPage(GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
        parent->SetKeyAt(parent_index, array[0].first);
        buffer_pool_manager->UnpinPage(GetParentPageId(), true);
    }

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
    INDEX_TEMPLATE_ARGUMENTS
    void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
            std::queue<BPlusTreePage *> *queue,
            BufferPoolManager *buffer_pool_manager) {
        for (int i = 0; i < GetSize(); i++) {
            auto *page = buffer_pool_manager->FetchPage(array[i].second);
            if (page == nullptr)
                throw Exception(EXCEPTION_TYPE_INDEX,
                                "all page are pinned while printing");
            BPlusTreePage *node =
                    reinterpret_cast<BPlusTreePage *>(page->GetData());
            queue->push(node);
        }
    }

    INDEX_TEMPLATE_ARGUMENTS
    std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
        if (GetSize() == 0) {
            return "";
        }
        std::ostringstream os;
        if (verbose) {
            os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
               << "]<" << GetSize() << "> ";
        }

        int entry = verbose ? 0 : 1;
        int end = GetSize();
        bool first = true;
        while (entry < end) {
            if (first) {
                first = false;
            } else {
                os << " ";
            }
            os << std::dec << array[entry].first.ToString();
            if (verbose) {
                os << "(" << array[entry].second << ")";
            }
            ++entry;
        }
        return os.str();
    }

// value type for internalNode should be page id_t
    template
    class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
            GenericComparator<4>>;

    template
    class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
            GenericComparator<8>>;

    template
    class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
            GenericComparator<16>>;

    template
    class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
            GenericComparator<32>>;

    template
    class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
            GenericComparator<64>>;
}  // namespace cmudb
