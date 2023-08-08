//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>
INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator(LeafPage *leaf, int pos, const KeyComparator &comparator, BufferPoolManager *buffer_pool_manager,
                INDEXITERATOR_TYPE *end_iterator)
      : leaf_(leaf),
        pos_(pos),
        comparator_(comparator),
        buffer_pool_manager_(buffer_pool_manager),
        end_iterator_(end_iterator) {}
  ~IndexIterator();  // NOLINT
  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (leaf_ == nullptr) {
      return itr.leaf_ == nullptr && pos_ == 0 && itr.pos_ == 0;
    }
    if (itr.leaf_ == nullptr) {
      return leaf_ == nullptr && pos_ == 0 && itr.pos_ == 0;
    }

    bool res = leaf_->GetPageId() == itr.leaf_->GetPageId();
    return res && pos_ == itr.pos_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  LeafPage *leaf_;
  int pos_;  // the position of the k-v pairs in the node
  KeyComparator comparator_;
  BufferPoolManager *buffer_pool_manager_;
  INDEXITERATOR_TYPE *end_iterator_;
};

}  // namespace bustub
