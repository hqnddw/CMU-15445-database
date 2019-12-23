/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

    INDEX_TEMPLATE_ARGUMENTS
    BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                              BufferPoolManager *buffer_pool_manager,
                              const KeyComparator &comparator,
                              page_id_t root_page_id)
            : index_name_(name),
              root_page_id_(root_page_id),
              buffer_pool_manager_(buffer_pool_manager),
              comparator_(comparator) {}

/**
 * Helper function to decide whether current b+tree is empty
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::IsEmpty() const {
        return root_page_id_ == INVALID_PAGE_ID;
    }

    template <typename KeyType, typename ValueType, typename KeyComparator>
    thread_local int BPlusTree<KeyType, ValueType, KeyComparator>::rootLockedCnt =
            0;
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * 查找与key相对应的value值，如果value存在，将得到的值赋给value，返回true，否则返回false
 * 1.找到leaf page
 * 2.用leaf page中的Lookup方法，找到key对应的value，用leaf page中的Lookup方法
 * 3.取消固定leaf page
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                                  std::vector<ValueType> &result,
                                  Transaction *transaction) {
        B_PLUS_TREE_LEAF_PAGE_TYPE *tar =
                FindLeafPage(key, false, OpType::READ, transaction);
        if (tar == nullptr) return false;
        result.resize(1);
        auto ret = tar->Lookup(key, result[0], comparator_);
        FreePagesInTransaction(false, transaction, tar->GetPageId());
        // buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
        // assert(buffer_pool_manager_->CheckAllUnpined());
        return ret;
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
/**
 * 首先判断树是不是空的，如果是空的，就创建一颗新树。如果不是就把值插到LEAFPAGE里。
 * 创建新树，就是开个NEW PAGE，随后把它的PAGE ID赋值给ROOT PAGE
 * ID，然后再把第一个KEY VALUE PAIR插入进去。
 * INSERT INTO LEAF PAGE，大概思路是先查找。值存在，就RETURN FALSE。
 * 不存在就插入到LEAF PAGE，如果满就分裂。 分裂就是后半部分的节点放进一个新的PAGE里，如果是INTERNAL
 * PAGE需要修改孩子的PARENT指针，如果是叶子PAGE需要更新NEXT 指针
 * 分裂完之后，会有一个新的节点插入到PARENT那，如果有需要递归做这件事。
*/
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                Transaction *transaction) {
        LockRootPageId(true);
        if (IsEmpty()) {
            StartNewTree(key, value);
            TryUnlockRootPageId(true);
            return true;
        }
        TryUnlockRootPageId(true);
        bool res = InsertIntoLeaf(key, value, transaction);
        // assert(Check());
        return res;
    }

/**
 * 将键值对插入到空树中
 * 1.用户首先需要buffer pool manager申请一个新的页面（如果返回空指针，则抛出“内存不足”异常）
 * 2.更新b+树的root page_id
 * 3.将键值对插入到leaf page中,取消page的固定，用到了leaf page的方法Insert
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
        page_id_t newPageId;
        //1
        Page *rootPage = buffer_pool_manager_->NewPage(newPageId);
        assert(rootPage != nullptr);

        //2
        B_PLUS_TREE_LEAF_PAGE_TYPE *root =
                reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(rootPage->GetData());
        root->Init(newPageId, INVALID_PAGE_ID);
        root_page_id_ = newPageId;
        UpdateRootPageId(true);

        //3
        root->Insert(key, value, comparator_);
        buffer_pool_manager_->UnpinPage(newPageId, true);
    }


/**
 * 将键值对插入到leaf page中
 * 1.用户需要先找到正确的leaf page作为插入目标，使用FindLeafPage方法
 * 2.接着遍历整个leaf page查看要插入的key是否存在，使用leaf page的Lookup方法
 *   2.1.如果存在返回false
 *   2.2.如果不存在，插入给定的键值对，并进入下一步
 * 3.判断当前leaf page的size是否大于maxSize
 *   3.1.如果大于，进行分裂，使用Split方法
 *   3.2.如果不大于，返回true
*/
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                        Transaction *transaction) {
        B_PLUS_TREE_LEAF_PAGE_TYPE *leafPage =
                FindLeafPage(key, false, OpType::INSERT, transaction);
        ValueType v;
        bool exist = leafPage->Lookup(key, v, comparator_);
        if (exist) {
            // buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
            FreePagesInTransaction(true, transaction);
            return false;
        }
        leafPage->Insert(key, value, comparator_);
        // 处理leaf page分裂的情况
        if (leafPage->GetSize() > leafPage->GetMaxSize()) {
            B_PLUS_TREE_LEAF_PAGE_TYPE *newLeafPage =
                    Split(leafPage, transaction);  // unpin it in below func
            InsertIntoParent(leafPage, newLeafPage->KeyAt(0), newLeafPage,
                             transaction);
        }
        // buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
        FreePagesInTransaction(true, transaction);
        return true;
    }

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
/**
 * 拆分输入页面并返回新创建的页面，使用模板N表示内部页面或叶子页面
 * 1.用户先从buffer pool manager申请一个新页面
 * 2.把输入页面的一半元素移动到新创建的页面中，用到了MoveHalfTo方法
 * 3.返回新页面
*/
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
        //1.
        page_id_t newPageId;
        Page *const newPage = buffer_pool_manager_->NewPage(newPageId);
        assert(newPage != nullptr);
        newPage->WLatch();
        transaction->AddIntoPageSet(newPage);
        //2.
        N *newNode = reinterpret_cast<N *>(newPage->GetData());
        newNode->Init(newPageId, node->GetParentPageId());
        node->MoveHalfTo(newNode, buffer_pool_manager_);
        //3.
        return newNode;
    }


/**
 * 拆分后将键值对插入到internal page中
 * @param   old_node      Split方法的输入页面
 * @param   key
 * @param   new_node      Split方法的返回页面
 * 1.判断old_page是否为根页面
 *   1.1.如果old_page为根页面，则申请一个新页面作为tree的根页面，
 *       并用PopulateNewRoot方法填充新的根页面，并且更新old_node
 *       和new_node的parent_id
 *   1.2.如果old_page不是根页面，找到old_node的parent page，并把new_node的
 *       parent_id设为old_node的parent_id，在parent page的后面插入key和new_node
 *       的page_id（用internal page的InsertNodeAfter方法），如果parent page满了，则继续递归的分裂
*/
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                          const KeyType &key,
                                          BPlusTreePage *new_node,
                                          Transaction *transaction) {
        if (old_node->IsRootPage()) {
            Page *const newPage = buffer_pool_manager_->NewPage(root_page_id_);
            assert(newPage != nullptr);
            assert(newPage->GetPinCount() == 1);
            B_PLUS_TREE_INTERNAL_PAGE *newRoot =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(newPage->GetData());
            newRoot->Init(root_page_id_);
            //填充新的root page
            newRoot->PopulateNewRoot(old_node->GetPageId(), key,
                                     new_node->GetPageId());
            // 更新leaf page的parent id
            old_node->SetParentPageId(root_page_id_);
            new_node->SetParentPageId(root_page_id_);
            // 此时默认为false，只需要更新page header里的root信息，不需要插入
            UpdateRootPageId();
            buffer_pool_manager_->UnpinPage(newRoot->GetPageId(), true);
            return;
        }
        page_id_t parentId = old_node->GetParentPageId();
        auto *page = FetchPage(parentId);
        assert(page != nullptr);
        B_PLUS_TREE_INTERNAL_PAGE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
        new_node->SetParentPageId(parentId);
        //在parent page的末尾插入键值对
        parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if (parent->GetSize() > parent->GetMaxSize()) {
            // parent page的节点满了，需要继续分裂
            B_PLUS_TREE_INTERNAL_PAGE *newLeafPage =
                    Split(parent, transaction);  // new page need unpin
            InsertIntoParent(parent, newLeafPage->KeyAt(0), newLeafPage,
                             transaction);
        }
        buffer_pool_manager_->UnpinPage(parentId, true);
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

/**
 * 删除与输入key关联的键值对
 * 1.如果当前树为空，请立即返回，否则下一步
 * 2.首先找到正确的leaf page作为目标页，然后从leaf page中删除键值对
 *  （用到了leaf page中的RemoveAndDeleteRecord方法）
 * 3.如果当前页删除后的size小于minSize，则进行redistribute或merge
*/
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
        if (IsEmpty()) return;
        B_PLUS_TREE_LEAF_PAGE_TYPE *delTar =
                FindLeafPage(key, false, OpType::DELETE, transaction);

        // leaf page中删除一个节点的方法，用memmove函数覆盖
        int curSize = delTar->RemoveAndDeleteRecord(key, comparator_);
        // 删除目标节点后，leaf
        // page中的节点个数小于minsize，向兄弟page中借或者与兄弟page合并
        if (curSize < delTar->GetMinSize()) {
            CoalesceOrRedistribute(delTar, transaction);
        }
        FreePagesInTransaction(true, transaction);
        // assert(Check());
    }

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
/**
 * 第一件如果来的是ROOT PAGE，要做ADJUST ROOT。
 * 如果不是，首要要找到这个PAGE的兄弟PAGE。
 * 然后按照兄弟PAGE和当前PAGE的位置关系，来做2个事情。第一个是当可以合并的时候，走Coalesce。
 * 如果不能合并，就意味着可以借节点。走Redistribute
*/
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>

    bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
        // if (N is the root and N has only one remaining child)
        // 当前leaf page中至少还有一个节点
        if (node->IsRootPage()) {
            // make the child of N the new root of the tree and delete N
            bool delOldRoot = AdjustRoot(node);
            if (delOldRoot) {
                transaction->AddIntoDeletedPageSet(node->GetPageId());
            }
            return delOldRoot;
        }
        // Let N2 be the previous or next child of parent(N)
        // 找当前页的前一页即左边的页，只有当当前页为首页时，才找右边的页
        N *node2;
        bool isRightSib = FindLeftSibling(node, node2, transaction);
        BPlusTreePage *parent = FetchPage(node->GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parentPage =
                static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(parent);
        // if (entries in N and N2 can fit in a single node)
        if (node->GetSize() + node2->GetSize() <= node->GetMaxSize()) {
            if (isRightSib) {
                swap(node, node2);
            }  // assumption node is after node2
            // 合并时，要把两个合并页的共同父节点删除掉
            int removeIndex = parentPage->ValueIndex(node->GetPageId());
            Coalesce(node2, node, parentPage, removeIndex,
                     transaction);  // unpin node,node2
            buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), true);
            return true;
        }
        /* Redistribution: 从兄弟页面借一个元素 */
        int nodeInParentIndex = parentPage->ValueIndex(node->GetPageId());
        Redistribute(node2, node, nodeInParentIndex);  // unpin node,node2
        buffer_pool_manager_->UnpinPage(parentPage->GetPageId(), false);
        return false;
    }

    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    bool BPLUSTREE_TYPE::FindLeftSibling(N *node, N *&sibling,
                                         Transaction *transaction) {
        auto page = FetchPage(node->GetParentPageId());
        B_PLUS_TREE_INTERNAL_PAGE *parent =
                reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page);
        int index = parent->ValueIndex(node->GetPageId());
        int siblingIndex = index - 1;
        if (index == 0) {  // no left sibling
            siblingIndex = index + 1;
        }
        //获取兄弟页
        sibling = reinterpret_cast<N *>(CrabingProtocalFetchPage(
                parent->ValueAt(siblingIndex), OpType::DELETE, -1, transaction));
        buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
        return index == 0;  // index == 0 意味着是右兄弟页面
    }

/**
 * 将当前页所有的元素移动到它的兄弟页中，并通知buffer pool manager删除当前页，
 * 如有必要，要递归地合并或分配
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happend
 */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    bool BPLUSTREE_TYPE::Coalesce(
            N *&neighbor_node, N *&node,
            BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
            int index, Transaction *transaction) {
        // assumption neighbor_node is before node
        assert(node->GetSize() + neighbor_node->GetSize() <= node->GetMaxSize());
        // 把当前页所有的元素移动到兄弟页面中
        node->(neighbor_node, index, buffer_pool_manager_);
        transaction->AddIntoDeletedPageSet(node->GetPageId());
        parent->Remove(index);
        //parent page此时一定是内部页，而内部页的第一个键是无效的，所以当
        //parent page中的节点个数 等于 Minsize时也需要合并或分配
        if (parent->GetSize() <= parent->GetMinSize()) {
            return CoalesceOrRedistribute(parent, transaction);
        }
        return false;
    }

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
    INDEX_TEMPLATE_ARGUMENTS
    template <typename N>
    void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
        if (index == 0) {
            neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
        } else {
            neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
        }
    }
/**
 * 如有必要，更新根页面
 * case 1: 删除根页面中的最后一个元素，但根页面仍然有最后一个孩子
 * case 2: 删除整个b+树的最后一个元素时
 */
    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
        if (old_root_node->IsLeafPage()) {  // case 2
            assert(old_root_node->GetSize() == 0);
            assert(old_root_node->GetParentPageId() == INVALID_PAGE_ID);
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId();
            return true;
        }
        if (old_root_node->GetSize() == 1) {  // case 1
            B_PLUS_TREE_INTERNAL_PAGE *root =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(old_root_node);
            const page_id_t newRootId = root->RemoveAndReturnOnlyChild();
            root_page_id_ = newRootId;
            UpdateRootPageId();
            // set the new root's parent id "INVALID_PAGE_ID"
            Page *page = buffer_pool_manager_->FetchPage(newRootId);
            assert(page != nullptr);
            B_PLUS_TREE_INTERNAL_PAGE *newRoot =
                    reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(page->GetData());
            newRoot->SetParentPageId(INVALID_PAGE_ID);
            buffer_pool_manager_->UnpinPage(newRootId, true);
            return true;
        }
        return false;
    }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
        KeyType useless;
        auto start_leaf = FindLeafPage(useless, true);
        TryUnlockRootPageId(false);
        return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
    }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
    INDEX_TEMPLATE_ARGUMENTS
    INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
        auto start_leaf = FindLeafPage(key);
        TryUnlockRootPageId(false);
        if (start_leaf == nullptr) {
            return INDEXITERATOR_TYPE(start_leaf, 0, buffer_pool_manager_);
        }
        int idx = start_leaf->KeyIndex(key, comparator_);
        return INDEXITERATOR_TYPE(start_leaf, idx, buffer_pool_manager_);
    }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
    INDEX_TEMPLATE_ARGUMENTS
    B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(
            const KeyType &key, bool leftMost, OpType op, Transaction *transaction) {
        bool exclusive = (op != OpType::READ);
        LockRootPageId(exclusive);
        if (IsEmpty()) {
            TryUnlockRootPageId(exclusive);
            return nullptr;
        }
        //, you need to first fetch the page from buffer pool using its unique
        // page_id, then reinterpret cast to either
        // a leaf or an internal page, and unpin the page after any writing or
        // reading operations.
        auto pointer = CrabingProtocalFetchPage(root_page_id_, op, -1, transaction);
        page_id_t next;
        // 找到leaf page
        for (page_id_t cur = root_page_id_; !pointer->IsLeafPage();
             pointer = CrabingProtocalFetchPage(next, op, cur, transaction),
                     cur = next) {
            B_PLUS_TREE_INTERNAL_PAGE *internalPage =
                    static_cast<B_PLUS_TREE_INTERNAL_PAGE *>(pointer);
            // 在实现迭代器的时候，要最先定位到左边的节点
            if (leftMost) {
                next = internalPage->ValueAt(0);
            } else {
                next = internalPage->Lookup(key, comparator_);
            }
        }
        // 之后查找会用到此页面，所以此时不用取消页面固定
        return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(pointer);
    }
    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
        auto page = buffer_pool_manager_->FetchPage(page_id);
        return reinterpret_cast<BPlusTreePage *>(page->GetData());
    }
    INDEX_TEMPLATE_ARGUMENTS
    BPlusTreePage *BPLUSTREE_TYPE::CrabingProtocalFetchPage(
            page_id_t page_id, OpType op, page_id_t previous,
            Transaction *transaction) {
        bool exclusive = op != OpType::READ;
        auto page = buffer_pool_manager_->FetchPage(page_id);
        Lock(exclusive, page);
        auto treePage = reinterpret_cast<BPlusTreePage *>(page->GetData());
        if (previous > 0 && (!exclusive || treePage->IsSafe(op))) {
            FreePagesInTransaction(exclusive, transaction, previous);
        }
        if (transaction != nullptr) transaction->AddIntoPageSet(page);
        return treePage;
    }

    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive,
                                                Transaction *transaction,
                                                page_id_t cur) {
        TryUnlockRootPageId(exclusive);
        if (transaction == nullptr) {
            assert(!exclusive && cur >= 0);
            Unlock(false, cur);
            buffer_pool_manager_->UnpinPage(cur, false);
            return;
        }
        for (Page *page : *transaction->GetPageSet()) {
            int curPid = page->GetPageId();
            Unlock(exclusive, page);
            buffer_pool_manager_->UnpinPage(curPid, exclusive);
            if (transaction->GetDeletedPageSet()->find(curPid) !=
                transaction->GetDeletedPageSet()->end()) {
                buffer_pool_manager_->DeletePage(curPid);
                transaction->GetDeletedPageSet()->erase(curPid);
            }
        }
        assert(transaction->GetDeletedPageSet()->empty());
        transaction->GetPageSet()->clear();
    }

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method every time root page id is changed.
 * @parameter: insert_record    default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
// insert_record  default  value is false. When set to true,
// insert a record <index_name, root_page_id> into header page instead of
// updating it
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
        HeaderPage *header_page = static_cast<HeaderPage *>(
                buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
        // header page 记录了tree的meta-data
        if (insert_record)
            // create a new record<index_name + root_page_id> in header_page
            header_page->InsertRecord(index_name_, root_page_id_);
        else
            // update root_page_id in header_page
            header_page->UpdateRecord(index_name_, root_page_id_);
        buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
    }

/*
 * This method is used for debug only
 * print out whole b+tree structure, rank by rank
 */
    INDEX_TEMPLATE_ARGUMENTS
    std::string BPLUSTREE_TYPE::ToString(bool verbose) {
        if (IsEmpty()) {
            return "Empty tree";
        }
        std::queue<BPlusTreePage *> todo, tmp;
        std::stringstream tree;
        auto node = reinterpret_cast<BPlusTreePage *>(
                buffer_pool_manager_->FetchPage(root_page_id_));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while printing");
        }
        todo.push(node);
        bool first = true;
        while (!todo.empty()) {
            node = todo.front();
            if (first) {
                first = false;
                tree << "| ";
            }
            // leaf page, print all key-value pairs
            if (node->IsLeafPage()) {
                auto page = reinterpret_cast<
                        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
                tree << page->ToString(verbose) << "(" << node->GetPageId()
                     << ")| ";
            } else {
                auto page = reinterpret_cast<
                        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
                        node);
                tree << page->ToString(verbose) << "(" << node->GetPageId()
                     << ")| ";
                page->QueueUpChildren(&tmp, buffer_pool_manager_);
            }
            todo.pop();
            if (todo.empty() && !tmp.empty()) {
                todo.swap(tmp);
                tree << '\n';
                first = true;
            }
            // unpin node when we are done
            buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
        }
        return tree.str();
    }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                        Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;

            KeyType index_key;
            index_key.SetFromInteger(key);
            RID rid(key);
            Insert(index_key, rid, transaction);
        }
    }
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
    INDEX_TEMPLATE_ARGUMENTS
    void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                        Transaction *transaction) {
        int64_t key;
        std::ifstream input(file_name);
        while (input) {
            input >> key;
            KeyType index_key;
            index_key.SetFromInteger(key);
            Remove(index_key, transaction);
        }
    }

/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

    INDEX_TEMPLATE_ARGUMENTS
    int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
        if (IsEmpty()) return true;
        auto node =
                reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while isBalanced");
        }
        int ret = 0;
        if (!node->IsLeafPage()) {
            auto page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE *>(node);
            int last = -2;
            for (int i = 0; i < page->GetSize(); i++) {
                int cur = isBalanced(page->ValueAt(i));
                if (cur >= 0 && last == -2) {
                    last = cur;
                    ret = last + 1;
                } else if (last != cur) {
                    ret = -1;
                    break;
                }
            }
        }
        buffer_pool_manager_->UnpinPage(pid, false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid, pair<KeyType, KeyType> &out) {
        if (IsEmpty()) return true;
        auto node =
                reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid));
        if (node == nullptr) {
            throw Exception(EXCEPTION_TYPE_INDEX,
                            "all page are pinned while isPageCorr");
        }
        bool ret = true;
        if (node->IsLeafPage()) {
            auto page = reinterpret_cast<
                    BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            for (int i = 1; i < size; i++) {
                if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
                    ret = false;
                    break;
                }
            }
            out = pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
        } else {
            auto page = reinterpret_cast<
                    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
            int size = page->GetSize();
            ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
            pair<KeyType, KeyType> left, right;
            for (int i = 1; i < size; i++) {
                if (i == 1) {
                    ret = ret && isPageCorr(page->ValueAt(0), left);
                }
                ret = ret && isPageCorr(page->ValueAt(i), right);
                ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 &&
                              comparator_(page->KeyAt(i), right.first) <= 0);
                ret = ret && (i == 1 ||
                              comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
                if (!ret) break;
                left = right;
            }
            out = pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
        }
        buffer_pool_manager_->UnpinPage(pid, false);
        return ret;
    }

    INDEX_TEMPLATE_ARGUMENTS
    bool BPLUSTREE_TYPE::Check(bool forceCheck) {
        if (!forceCheck && !openCheck) {
            return true;
        }
        pair<KeyType, KeyType> in;
        bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
        bool isBal = (isBalanced(root_page_id_) >= 0);
        bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
        if (!isPageInOrderAndSizeCorr)
            cout << "problem in page order or page size" << endl;
        if (!isBal) cout << "problem in balance" << endl;
        if (!isAllUnpin) cout << "problem in page unpin" << endl;
        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
    }

    template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
    template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
    template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
    template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
    template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace cmudb
