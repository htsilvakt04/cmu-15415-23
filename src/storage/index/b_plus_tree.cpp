#include "storage/index/b_plus_tree.h"
#include <string>
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/header_page.h"
namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  end_iterator_ = new INDEXITERATOR_TYPE(nullptr, 0, comparator_, buffer_pool_manager_, nullptr);
}
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::~BPlusTree() { delete end_iterator_; }
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto node = buffer_pool_manager_->FetchPage(root_page_id_);
  if (node == nullptr) {
    return true;
  }

  auto *root = reinterpret_cast<InternalPage *>(node);
  bool res = root == nullptr || root->GetSize() == 0;
  if (node != nullptr) {
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
  }

  return res;
}

// safe === the page p is not full for insertion && is not under-full for deletion
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(Page *p, enum OPERATION operation) -> bool {
  bool res = true;
  if (operation == OPERATION::SEARCH) {
    return res;
  }
  auto node = reinterpret_cast<BPlusTreePage *>(p);
  if (node->IsLeafPage()) {
    auto leaf = reinterpret_cast<LeafPage *>(node);
    if (operation == OPERATION::INSERT) {
      res = leaf->GetSize() < leaf_max_size_ - 1;
    } else if (operation == OPERATION::DELETE) {
      res = leaf->GetSize() > leaf_max_size_ / 2;
    }
  } else {
    auto internal_page = reinterpret_cast<InternalPage *>(node);
    if (operation == OPERATION::INSERT) {
      res = internal_page->GetSize() < internal_max_size_ - 1;
    } else if (operation == OPERATION::DELETE) {
      res = internal_page->GetSize() > internal_max_size_ / 2 + 1;
    }
  }

  return res;
}
// Release both the global_latch_ and all parent latches acquired during the FindLeaf()
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseLatches(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    Page *page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      global_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

/// Find the leaf node containing the search_key.
/// The caller (GetValue() and Insert()) must acquired the global_lock before calling this.
/// Remember to unpin the page after using it
/// \tparam KeyType
/// \tparam ValueType
/// \tparam KeyComparator
/// \param search_key
/// \return LeafPage *
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &search_key, Transaction *transaction, enum OPERATION opt) -> Page * {
  bool read = opt == OPERATION::SEARCH;
  Page *parent_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *parent_node = reinterpret_cast<BPlusTreePage *>(parent_page);
  if (read) {
    global_latch_.RUnlock();
  }

  // acquire the parent latch
  if (read) {
    parent_page->RLatch();
  } else {
    parent_page->WLatch();
  }

  // find the leaf contains search_key
  while (!parent_node->IsLeafPage()) {
    page_id_t child_page_id = INVALID_PAGE_ID;
    bool found = false;
    int i;
    auto parent_node_tmp = reinterpret_cast<InternalPage *>(parent_page);
    // traverse the whole keys to find the first key >= search_key
    for (i = 0; i < parent_node_tmp->GetSize(); ++i) {
      if (comparator_(parent_node_tmp->KeyAt(i), search_key) > 0) {
        found = true;
        break;
      }
    }  // end for
    if (!found) {
      // take the last pointer, and move to the subtree there
      child_page_id = parent_node_tmp->ValueAt(parent_node_tmp->GetSize() - 1);
    } else {  // take the left subtree
      child_page_id = parent_node_tmp->ValueAt(i - 1);
    }

    // found the child
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page);

    // i) acquire the latch of the child
    // ii) Check safe condition, then release if it is
    if (read) {
      child_page->RLatch();
      // release parent immediately since this is a read
      parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(parent_page);
      if (IsSafe(child_page, opt)) {
        // release parent latch
        ReleaseLatches(transaction);
      }
    }
    // advance
    parent_page = child_page;
    parent_node = child_node;
  }  // end while

  return parent_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &search_key, std::vector<ValueType> *result, Transaction *transaction)
    -> bool {
  std::vector<ValueType> res;
  // acquire global lock
  global_latch_.RLock();
  auto *node = FindLeaf(search_key, transaction, OPERATION::SEARCH);
  auto leaf = reinterpret_cast<LeafPage *>(node);
  // scan it
  int i;
  for (i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(leaf->KeyAt(i), search_key) == 0) {
      res.push_back(leaf->ValueAt(i));
      break;
    }
  }
  // release the latch
  node->RUnlatch();
  // unpin
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  *result = std::move(res);
  return !result->empty();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertSorted(std::vector<KeyType> &keys, std::vector<page_id_t> &values, const KeyType &key,
                                  const page_id_t value) {
  auto it = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
  auto index = std::distance(keys.begin(), it);
  keys.insert(it, key);
  values.insert(values.begin() + index, value);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewTree(const KeyType &key, const ValueType &value) -> LeafPage * {
  // create an empty leaf node L, which is also a new root
  page_id_t page_id;
  auto node = buffer_pool_manager_->NewPage(&page_id);
  if (node == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Can't malloc page");
  }
  auto p = node->GetPageId();
  auto *l = reinterpret_cast<LeafPage *>(node);
  l->Init(p, INVALID_PAGE_ID, leaf_max_size_);
  l->SetNextPageId(INVALID_PAGE_ID);
  SetRootPageId(p);
  UpdateRootPageId(0);

  // create new k-v pair
  l->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(p, true);
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::CreateNewNonLeafTree(BPlusTreePage *L, BPlusTreePage *L_, const KeyType &key) -> page_id_t {
  InternalPage *p_page;
  page_id_t page_id;
  auto p_page_tmp = buffer_pool_manager_->NewPage(&page_id);
  if (p_page_tmp == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Can't malloc page");
  }
  p_page = reinterpret_cast<InternalPage *>(p_page_tmp);
  p_page->Init(page_id, INVALID_PAGE_ID, internal_max_size_);
  // the left most pointer
  p_page->Insert(KeyType{}, L->GetPageId(), comparator_);
  // the new pointer
  p_page->Insert(key, L_->GetPageId(), comparator_);
  SetRootPageId(page_id);
  UpdateRootPageId(0);

  L->SetParentPageId(page_id);
  L_->SetParentPageId(page_id);
  buffer_pool_manager_->UnpinPage(p_page->GetPageId(), true);
  return page_id;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // acquire the global lock
  global_latch_.WLock();
  // add the acquired W global_latch_ into the list
  transaction->AddIntoPageSet(nullptr);  // learn from a student, null == the global_latch_.
  if (IsEmpty()) {
    CreateNewTree(key, value);
    ReleaseLatches(transaction);
    return true;
  }

  // we might release the global lock here if the insertion is safe for the root
  auto l = FindLeaf(key, transaction, OPERATION::INSERT);
  return InsertInLeaf(l, key, value, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename T>
auto BPLUSTREE_TYPE::Split(T *L) -> T * {
  auto node = reinterpret_cast<BPlusTreePage *>(L);
  page_id_t k;
  auto page = buffer_pool_manager_->NewPage(&k);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Can't malloc page");
  }

  auto *l_prime_tmp = reinterpret_cast<T *>(page);
  if (node->IsLeafPage()) {
    auto l_node = reinterpret_cast<LeafPage *>(L);
    auto *l_prime = reinterpret_cast<LeafPage *>(l_prime_tmp);
    l_prime->SetPageType(IndexPageType::LEAF_PAGE);
    l_prime->Init(page->GetPageId(), INVALID_PAGE_ID, leaf_max_size_);

    l_node->MoveHalfInto(l_prime, comparator_);
  } else {
    // for internal page
    auto l_node = reinterpret_cast<InternalPage *>(L);
    auto *l_prime = reinterpret_cast<InternalPage *>(l_prime_tmp);
    l_prime->SetPageType(IndexPageType::INTERNAL_PAGE);
    l_prime->Init(page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);

    l_node->MoveHalfInto(l_prime, comparator_);
  }
  return l_prime_tmp;
}
/// Insert key-value into this leaf
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInLeaf(Page *node, const KeyType &key, const ValueType &value, Transaction *transaction)
    -> bool {
  auto *left_l = reinterpret_cast<LeafPage *>(node);
  // we're holding the W lock of L
  bool res = left_l->Insert(key, value, comparator_);

  // duplicate key: we release all stuff
  if (!res) {
    ReleaseLatches(transaction);
    // release the lock of L
    node->WUnlatch();
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return res;
  }

  // the leaf is not full: Since we inserted at top, this means we're done.
  if (!left_l->IsFull()) {
    // release the latch (both global_latch_ and the parent latches)
    ReleaseLatches(transaction);
    // release the lock of L
    node->WUnlatch();
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return res;
  }

  // split the L node due to it is full and it is a leaf
  auto *right_l = Split(left_l);
  // set sibling pointers
  right_l->SetNextPageId(left_l->GetNextPageId());
  left_l->SetNextPageId(right_l->GetPageId());
  KeyType k = right_l->KeyAt(0);

  InsertInParent(left_l, k, right_l, transaction);
  right_l->SetParentPageId(left_l->GetParentPageId());
  // release the latch
  ReleaseLatches(transaction);
  node->WUnlatch();
  // remember to unpin the page and set dirty flag
  buffer_pool_manager_->UnpinPage(left_l->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(right_l->GetPageId(), true);
  return true;
}
// add the (key, L_) to parent of L
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *L, const KeyType &key, BPlusTreePage *L_, Transaction *transaction) {
  // If L is root, then it means we need to create a new root
  if (L->IsRootPage()) {
    CreateNewNonLeafTree(L, L_, key);
    return;
  }
  InternalPage *p_prime_node = nullptr;
  InternalPage *parent_node = Parent(L);

  if (!parent_node->IsFull()) {
    parent_node->Insert(key, L_->GetPageId(), comparator_);
  } else {
    // if it is full => we need to split
    auto n = sizeof(MappingType) * (parent_node->GetSize() + 1);
    // create a temp node to do the split
    char tmp_buff[INTERNAL_PAGE_HEADER_SIZE + n];
    auto *tmp_page = reinterpret_cast<InternalPage *>(tmp_buff);
    // copy elems from parent_node to the tmp_page
    auto *parent_page = reinterpret_cast<Page *>(parent_node);
    memcpy(tmp_buff, parent_page->GetData(), (n - sizeof(MappingType)) + INTERNAL_PAGE_HEADER_SIZE);
    // insert (key, L_) to the tmp_page in sorted order
    tmp_page->Insert(key, L_->GetPageId(), comparator_);
    // split the (n + 1) items node-> Return the reference to the new node
    auto *p_prime_page = Split(tmp_page);
    p_prime_node = reinterpret_cast<InternalPage *>(p_prime_page);
    // copy back
    memcpy(parent_page->GetData(), tmp_buff, tmp_page->GetSize() * sizeof(MappingType) + INTERNAL_PAGE_HEADER_SIZE);
    ReassignParentLinks(parent_node, p_prime_node);
    // clear the key at 0 for the new page
    KeyType k{p_prime_node->KeyAt(0)};
    p_prime_node->SetKeyAt(0, KeyType{});
    // set the parent for the new node
    p_prime_node->SetParentPageId(parent_node->GetParentPageId());
    // insert the key to parent
    InsertInParent(parent_node, k, p_prime_node, transaction);
  }

  if (p_prime_node != nullptr) {
    buffer_pool_manager_->UnpinPage(p_prime_node->GetPageId(), true);
  }
  // unpin parent_node
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReassignParentLinks(InternalPage *p_node, InternalPage *p_prime_node) {
  // reassign parent links for child
  for (int i = 0; i < p_node->GetSize(); ++i) {
    auto p = buffer_pool_manager_->FetchPage(p_node->ValueAt(i));
    auto *child = reinterpret_cast<BPlusTreePage *>(p);
    bool dirty = false;
    if (child->GetParentPageId() != p_node->GetPageId()) {
      child->SetParentPageId(p_node->GetPageId());
      dirty = true;
    }
    buffer_pool_manager_->UnpinPage(child->GetPageId(), dirty);
  }
  // reassign parent links for child
  for (int i = 0; i < p_prime_node->GetSize(); ++i) {
    auto p = buffer_pool_manager_->FetchPage(p_prime_node->ValueAt(i));
    auto *child = reinterpret_cast<BPlusTreePage *>(p);
    bool dirty = false;
    if (child->GetParentPageId() != p_prime_node->GetPageId()) {
      child->SetParentPageId(p_prime_node->GetPageId());
      dirty = true;
    }
    buffer_pool_manager_->UnpinPage(child->GetPageId(), dirty);
  }
}
// Return the parent of 'node'
// the caller must unpin the page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Parent(const BPlusTreePage *node) -> InternalPage * {
  if (node->IsRootPage()) {
    return nullptr;
  }

  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  // hold the global lock
  global_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);

  auto *node = FindLeaf(key, transaction, OPERATION::DELETE);
  auto leaf = reinterpret_cast<LeafPage *>(node);
  // after the FindLeaf, we hold all the locks that we need
  // so we just do normal deletion, then release the latches after it is done.
  DeleteEntryLeaf(leaf, key, transaction);
  ReleaseLatches(transaction);
  node->WUnlatch();
  // remove deleted pages from the buffer poll
  // Retrieve the DeletedPageSet
  auto deleted_page_set = transaction->GetDeletedPageSet();
  // Iterate through the DeletedPageSet
  for (const auto &page_id : *deleted_page_set) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  // Clear the DeletedPageSet after deletion
  deleted_page_set->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReplaceKey(InternalPage *node, KeyType k1, KeyType k2) {
  for (int i = 0; i < node->GetSize(); ++i) {
    if (comparator_(node->KeyAt(i), k1) == 0) {
      node->SetKeyAt(i, k2);
      break;
    }
  }
}

// return the key in the parent of 'node' which points to 'node'
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyTo(LeafPage *node) -> KeyType {
  InternalPage *parent = Parent(node);
  for (int i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      return parent->KeyAt(i);
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  // never goes here
  return KeyType{};
}

// return the left sibling leaf node of 'node'
// return null if there is no left sibling
// the caller must unpin the page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeftSibling(LeafPage *node) -> LeafPage * {
  InternalPage *parent = Parent(node);
  if (parent == nullptr) {
    return nullptr;
  }

  int i;
  for (i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  if (i == 0) {
    return nullptr;
  }
  int where = parent->ValueAt(i - 1);
  return reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(where));
}

// return the key in the parent of 'node' which points to 'node'
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyTo(InternalPage *node) -> KeyType {
  InternalPage *parent = Parent(node);
  for (int i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
      return parent->KeyAt(i);
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  // never goes here
  return KeyType{};
}

// return the left sibling leaf node of 'node'
// return null if there is no left sibling
// the caller must unpin the page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeftSibling(InternalPage *node) -> InternalPage * {
  InternalPage *parent = Parent(node);
  if (parent == nullptr) {
    return nullptr;
  }
  int i;
  for (i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  if (i == 0) {
    return nullptr;
  }
  int where = parent->ValueAt(i - 1);
  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(where));
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RightSibling(InternalPage *node) -> InternalPage * {
  InternalPage *parent = Parent(node);
  if (parent == nullptr) {
    return nullptr;
  }
  int i;
  for (i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  if (i == parent->GetSize() - 1) {
    return nullptr;
  }

  int where = parent->ValueAt(i + 1);
  return reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(where));
}
// Function to perform redistribution of entries between two leaf nodes
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeLeaf(LeafPage *n, LeafPage *n_, KeyType K_) {
  LeafPage *n_page = n;
  LeafPage *n_prime_page = n_;
  auto rsib = RightSibling(n_page);
  // n is a predecessor of n_
  if (rsib != nullptr && rsib->GetPageId() == n_->GetPageId()) {
    // find the first k-v pair in N_
    std::pair<KeyType, ValueType> m{n_prime_page->KeyAt(0), n_prime_page->ValueAt(0)};
    // remove it from N_
    n_prime_page->Delete(m.first, comparator_);
    // insert this k-v pair into N, which then becomes the last k-v in the list
    n_page->Insert(m.first, m.second, comparator_);
    auto parent = Parent(n);
    // replace the key K_ in parent to the first key at n_
    ReplaceKey(parent, K_, n_prime_page->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    // find the last k-v pair in N_
    std::pair<KeyType, ValueType> m{n_prime_page->KeyAt(n_prime_page->GetSize() - 1),
                                    n_prime_page->ValueAt(n_prime_page->GetSize() - 1)};
    // remove the pair from n_prime_page
    n_prime_page->Delete(m.first, comparator_);
    // insert m as a first element in N
    n->Insert(m.first, m.second, comparator_);
    auto parent = Parent(n);
    // replace K by the first key in N in parent of N
    ReplaceKey(parent, K_, m.first);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }

  if (rsib != nullptr) {
    buffer_pool_manager_->UnpinPage(rsib->GetPageId(), false);
  }
}

// Merge N into N_
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeLeaf(LeafPage *n, LeafPage *n_, KeyType K_, Transaction *transaction) -> LeafPage * {
  // always assume N_ before N
  LeafPage *n_page = n;
  LeafPage *n_prime_page = n_;
  if (n->GetNextPageId() == n_->GetPageId()) {
    n_page = n_;
    n_prime_page = n;
  }

  // append all k-v pairs in N into N_
  for (int i = 0; i < n_page->GetSize(); ++i) {
    n_prime_page->Insert(n_page->KeyAt(i), n_page->ValueAt(i), comparator_);
  }
  // set next pointer of N_
  n_prime_page->SetNextPageId(n_page->GetNextPageId());
  n_page->SetParentPageId(INVALID_PAGE_ID);
  transaction->AddIntoDeletedPageSet(n_page->GetPageId());
  return n_prime_page;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::MergeInternalPage(InternalPage *n, InternalPage *n_, KeyType K_, Transaction *transaction)
    -> InternalPage * {
  InternalPage *n_page = n;
  InternalPage *n_prime_page = n_;
  auto rsb = RightSibling(n);
  if (rsb != nullptr && rsb->GetPageId() == n_->GetPageId()) {
    n_page = n_;
    n_prime_page = n;
  }

  // append (K_, leaf most pointer of n_page to n_prime_page)
  n_prime_page->Insert(K_, n_page->ValueAt(0), comparator_);
  // set the node of the leaf most pointer of n_page points to new parent
  auto child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(n_page->ValueAt(0)));
  child->SetParentPageId(n_prime_page->GetPageId());
  buffer_pool_manager_->UnpinPage(child->GetPageId(), true);

  // appends k_ and  all k-v pairs from n_page to n_prime
  for (int i = 1; i < n_page->GetSize(); ++i) {
    n_prime_page->Insert(n_page->KeyAt(i), n_page->ValueAt(i), comparator_);
    // set the parent pointer for the child
    child = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(n_page->ValueAt(i)));
    child->SetParentPageId(n_prime_page->GetPageId());
    buffer_pool_manager_->UnpinPage(child->GetPageId(), true);
  }
  if (rsb != nullptr) {
    buffer_pool_manager_->UnpinPage(rsb->GetPageId(), false);
  }
  transaction->AddIntoDeletedPageSet(n_page->GetPageId());
  n_page->SetParentPageId(INVALID_PAGE_ID);
  // return n_prime_page as a parent of the child
  return n_prime_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RedistributeInternalPage(InternalPage *n, InternalPage *n_, KeyType K_) {
  InternalPage *n_page = n;
  InternalPage *n_prime_page = n_;
  auto lsib = LeftSibling(n);
  // N_ is a predecessor of N
  if (lsib != nullptr && lsib->GetPageId() == n_->GetPageId()) {
    // Find the index 'm' such that N_.Pm is the last pointer in N_
    int m = n_prime_page->GetSize();
    KeyType key = n_prime_page->KeyAt(m - 1);
    page_id_t val = n_prime_page->ValueAt(m - 1);

    // remove the last k-v pair from n_prime_page
    n_prime_page->Delete(key, comparator_);

    // insert (K_, first pointer of N) to N
    n_page->Insert(K_, n_page->ValueAt(0), comparator_);
    // set the pointer at index 0 points to what Pm(val) points at.
    n_page->SetValueAt(0, val);
    // set the affected children to the new parent
    auto node1 = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(val));
    node1->SetParentPageId(n_page->GetPageId());
    buffer_pool_manager_->UnpinPage(node1->GetPageId(), true);

    auto parent = Parent(n);
    // replace K by the first key in N in parent of N
    ReplaceKey(parent, K_, key);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    // Find m such that N->Pm is the first pointer in N
    int m = 0;
    KeyType key = n_prime_page->KeyAt(m);
    page_id_t val = n_prime_page->ValueAt(m);

    // Remove (N'.Km, N.Pm) from N_
    n_prime_page->Delete(key, comparator_);

    // Insert (val, K') as the last pointer and value in N,
    // by shifting other pointers and values left
    n_page->Insert(K_, val, comparator_);
    auto child_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(val));
    child_page->SetParentPageId(n_page->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);

    auto parent = Parent(n);
    // Replace K' in parent(N) by N_.K1
    ReplaceKey(parent, K_, n_prime_page->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }

  if (lsib != nullptr) {
    buffer_pool_manager_->UnpinPage(lsib->GetPageId(), false);
  }
}

// Remove(key)
// leaf = FindLeaf(key)
// 	   -> DeleteEntry(leaf, key)
//		-> MergeLeaf(N, N_, K_)
//		    -> DeleteEntry(N, N_, K_)

// delete 'key' from 'N'
// return page_id of 'N' if there is no merge
// otherwise, return whatever the merge process return which is the parent page_id of the two merged nodes
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntryLeaf(LeafPage *N, const KeyType &key, Transaction *transaction) -> page_id_t {
  page_id_t res = N->GetPageId();
  if (!N->Delete(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(N->GetPageId(), false);
    return res;
  }

  // after deletion, there are enough k-v pairs in the node, then we're done
  if (N->IsRootPage() || !N->IsUnderFull()) {
    buffer_pool_manager_->UnpinPage(N->GetPageId(), true);
    return res;
  }

  // otherwise, the node has too few k-v pairs, then we need to merge/redistribute keys-values
  LeafPage *n = RightSibling(N);
  // then N_ is the left sibling
  KeyType k;
  if (n != nullptr) {
    k = KeyTo(n);
  } else {
    n = LeftSibling(N);
    k = KeyTo(N);
  }

  if ((N->GetSize() + n->GetSize()) <= leaf_max_size_) {
    // merge N to n
    auto new_node = MergeLeaf(N, n, k, transaction);
    // remove N from the parent
    auto parent = reinterpret_cast<InternalPage *>(Parent(new_node));
    res = DeleteEntryFromInternalPage(parent, k, transaction);
    // set parent for N_
    new_node->SetParentPageId(res);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    RedistributeLeaf(N, n, k);
  }
  buffer_pool_manager_->UnpinPage(N->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(n->GetPageId(), true);
  return res;
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DeleteEntryFromInternalPage(InternalPage *N, const KeyType &key, Transaction *transaction)
    -> page_id_t {
  page_id_t res = N->GetPageId();
  if (!N->Delete(key, comparator_)) {
    return res;
  }

  // N is root and has only one remaining child -> then make this child a new root and delete N
  if (N->IsRootPage() && N->GetSize() == 1) {
    transaction->AddIntoDeletedPageSet(root_page_id_);
    page_id_t child_pgid = N->ValueAt(0);
    SetRootPageId(child_pgid);
    UpdateRootPageId(child_pgid);
    return INVALID_PAGE_ID;
  }

  // after deletion, there are enough k-v pairs in the node, then we're done
  if (N->IsRootPage() || !N->IsUnderFull()) {
    return res;
  }

  // otherwise, the node has too few k-v pairs, then we need to merge/redistribute keys-values
  KeyType k = KeyTo(N);
  auto left_sib = LeftSibling(N);
  auto right_sib = RightSibling(N);
  // prefer left node for merge
  auto n_prime = left_sib != nullptr ? left_sib : right_sib;
  // merge
  if ((N->GetSize() + n_prime->GetSize()) <= internal_max_size_) {
    if (left_sib == nullptr) {
      k = KeyTo(n_prime);
    }
    // merge N to n_prime
    auto new_node = MergeInternalPage(N, n_prime, k, transaction);
    // Recursively delete N from its parent
    auto parent = reinterpret_cast<InternalPage *>(Parent(new_node));
    auto parent_page_id = DeleteEntryFromInternalPage(parent, k, transaction);

    res = new_node->GetPageId();
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  } else {
    if (left_sib == nullptr) {
      k = KeyTo(n_prime);
    }
    RedistributeInternalPage(N, n_prime, k);
  }
  if (left_sib != nullptr) {
    buffer_pool_manager_->UnpinPage(left_sib->GetPageId(), true);
  }
  if (right_sib != nullptr) {
    buffer_pool_manager_->UnpinPage(right_sib->GetPageId(), true);
  }
  return res;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  // Find the leftmost leaf page
  global_latch_.RLock();
  auto leaf_page = FindLeaf(KeyType{}, nullptr, OPERATION::SEARCH);
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_page);
  // Control should never reach here, but it's good to return something in case of errors.
  auto iter = INDEXITERATOR_TYPE(leaf_node, 0, comparator_, buffer_pool_manager_, end_iterator_);
  return iter;
}

// the caller must unpin the page
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftMostLeaf() -> BPlusTree::LeafPage * {
  global_latch_.RLock();
  auto *root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  // find the leaf contains search_key
  while (!root->IsLeafPage()) {
    page_id_t next_pg_id = root->ValueAt(0);
    // unpin
    buffer_pool_manager_->UnpinPage(root->GetPageId(), false);
    // advance
    root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(next_pg_id));
  }  // end while
  global_latch_.RUnlock();
  return reinterpret_cast<LeafPage *>(root);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RightSibling(LeafPage *node) -> LeafPage * {
  auto page = reinterpret_cast<BPlusTreePage *>(node);
  InternalPage *parent = Parent(page);
  if (parent == nullptr) {
    return nullptr;
  }
  int i;
  for (i = 0; i < parent->GetSize(); ++i) {
    if (parent->ValueAt(i) == node->GetPageId()) {
      break;
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
  if (i == parent->GetSize() - 1) {
    return nullptr;
  }

  int where = parent->ValueAt(i + 1);
  return reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(where));
}
/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  global_latch_.RLock();
  auto node = FindLeaf(key, nullptr, OPERATION::SEARCH);
  auto *leaf = reinterpret_cast<LeafPage *>(node);
  int pos = 0;
  // todo, do Bin-search
  for (int i = 0; i < leaf->GetSize(); ++i) {
    if (comparator_(leaf->KeyAt(i), key) == 0) {
      pos = i;
      break;
    }
  }

  auto iter = INDEXITERATOR_TYPE(leaf, pos, comparator_, buffer_pool_manager_, end_iterator_);
  return iter;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(nullptr, 0, comparator_, buffer_pool_manager_, end_iterator_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(page_id_t p) { root_page_id_ = p; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  header_page->WLatch();
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
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
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << '\n';
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)), bpm, out);
  out << "}" << '\n';
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i)));
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1)));
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << '\n';
    std::cout << '\n';
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << '\n';
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << '\n';
    std::cout << '\n';
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
