//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  max_size_ = max_size;
  page_type_ = IndexPageType::LEAF_PAGE;
  size_ = 0;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  // replace with your own code
  ValueType value{array_[index].second};
  return value;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, KeyComparator comparator_) -> bool {
  if (GetSize() == 0) {
    return false;
  }
  int i;
  for (i = 0; i < GetSize(); ++i) {
    if (comparator_(KeyAt(i), key) == 0) {
      break;
    }
  }
  // not found
  if (i == GetSize()) {
    return false;
  }
  // erase the pointer
  array_[i].second = ValueType{};
  for (int j = i + 1; j < GetSize(); ++j) {
    array_[j - 1] = array_[j];
  }
  SetSize(GetSize() - 1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsUnderFull() -> bool { return GetSize() < GetMaxSize() / 2; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsFull() -> bool { return GetSize() >= GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Clear() {
  for (int i = 0; i < GetSize(); ++i) {
    array_[i].first = KeyType();     // Reset the key
    array_[i].second = ValueType();  // Reset the value
  }
  // Set the CurrentSize to 0 to indicate the leaf page is empty
  SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfInto(BPlusTreeLeafPage *l_prime, KeyComparator comparator_) {
  int num = GetMinSize();
  auto l_prime_page = reinterpret_cast<Page *>(l_prime);
  auto l_prime_node = reinterpret_cast<BPlusTreeLeafPage *>(l_prime_page);
  auto l_where = GetSize() - num;
  for (int i = l_where; i < GetSize(); ++i) {
    l_prime_node->Insert(KeyAt(i), ValueAt(i), comparator_);
  }

  SetSize(l_where);
  l_prime_node->SetSize(num);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetData() -> std::pair<KeyType, ValueType> * { return array_; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator_) -> bool {
  int insert_idx = 0;

  int size = GetSize();
  // find the first position where the key of the node is >= key
  while (insert_idx < size && comparator_(KeyAt(insert_idx), key) < 0) {
    insert_idx++;
  }

  // return false if insert the same key
  if (insert_idx < size && comparator_(KeyAt(insert_idx), key) == 0) {
    return false;
  }

  // shift elements
  for (int i = size - 1; i >= insert_idx; i--) {
    array_[i + 1] = array_[i];
  }
  // set it
  array_[insert_idx] = std::make_pair(key, value);
  // update the size
  SetSize(size + 1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) -> MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::ShiftRight(int k) {
  k = k % GetSize();
  for (int i = k; i < GetSize(); ++i) {
    array_[i + 1] = array_[i];
  }
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
