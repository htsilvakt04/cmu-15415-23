//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  page_id_ = page_id;
  parent_page_id_ = parent_id;
  max_size_ = max_size;
  page_type_ = IndexPageType::INTERNAL_PAGE;
  size_ = 0;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 * For now, assume that the index is < size() of the array
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  KeyType key{array_[index].first};
  return key;
}
/// For now, assume that the index is < size() of the array
/// \param index
/// \param key
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType val{array_[index].second};
  return val;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsFull() -> bool { return GetSize() == GetMaxSize(); }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto k) { return comparator(pair.first, k) < 0; });
  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return std::prev(target)->second;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator comparator_) {
  int insert_idx = 0;
  int size = GetSize();

  // find the first position where the key of the node is >= key
  while (insert_idx < size && comparator_(KeyAt(insert_idx), key) < 0) {
    insert_idx++;
  }
  // shift elements
  for (int i = size - 1; i >= insert_idx; i--) {
    array_[i + 1] = array_[i];
  }
  // set it
  array_[insert_idx] = std::make_pair(key, value);
  // update the size
  SetSize(size + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Clear() {
  for (int i = 0; i < GetSize(); ++i) {
    array_[i].first = KeyType();     // Reset the key
    array_[i].second = ValueType();  // Reset the value
  }
  // Set the CurrentSize to 0 to indicate the leaf page is empty
  SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, KeyComparator comparator_) -> bool {
  if (GetSize() == 0) {
    return false;
  }

  bool found = false;
  int i;
  for (i = 0; i < GetSize(); ++i) {
    if (comparator_(KeyAt(i), key) == 0) {
      array_[i].first = KeyType{};
      array_[i].second = ValueType{};
      found = true;
      break;
    }
  }
  // not found
  if (!found) {
    return false;
  }

  for (int j = i + 1; j < GetSize(); ++j) {
    array_[j - 1] = array_[j];
  }

  SetSize(GetSize() - 1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfInto(BPlusTreeInternalPage *l_prime, KeyComparator comparator_) {
  int num = GetMinSize();
  auto l_prime_page = reinterpret_cast<Page *>(l_prime);
  auto l_prime_node = reinterpret_cast<BPlusTreeInternalPage *>(l_prime_page);

  auto l_where = GetSize() - num;
  for (int i = l_where; i < GetSize(); ++i) {
    l_prime_node->Insert(KeyAt(i), ValueAt(i), comparator_);
  }

  SetSize(l_where);
  l_prime_node->SetSize(num);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::IsUnderFull() -> bool { return GetSize() < (GetMaxSize() / 2) + 1; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
