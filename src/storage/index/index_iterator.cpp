/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

// Return whether this iterator is pointing at the last key/value pair.
INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // it must be at the last leaf and in the last position
  return leaf_->GetNextPageId() == INVALID_PAGE_ID && pos_ == leaf_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return this->leaf_->PairAt(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE {
  auto node = reinterpret_cast<Page *>(leaf_);
  node->RLatch();

  if (IsEnd()) {
    // If it is at the end, just return the iterator itself
    node->RUnlatch();
    return *end_iterator_;
  }

  // Increment the iterator to the next key-value pair
  pos_++;

  if (pos_ >= leaf_->GetSize()) {
    // If we have reached the end of the current leaf page, move to the next leaf page
    auto next_leaf_id = leaf_->GetNextPageId();
    node->RUnlatch();
    if (next_leaf_id != INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      auto next_page = buffer_pool_manager_->FetchPage(next_leaf_id);
      auto next_leaf = reinterpret_cast<LeafPage *>(next_page);
      // reassign
      leaf_ = next_leaf;
      pos_ = 0;  // Reset the position to the beginning of the next leaf page
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    } else {
      leaf_ = nullptr;
      pos_ = 0;
    }
  }

  // Step 3: Return the iterator itself after the increment
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
