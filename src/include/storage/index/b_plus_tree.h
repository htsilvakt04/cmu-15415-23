//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {
enum OPERATION { SEARCH, INSERT, DELETE };
#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  ~BPlusTree();
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;
  auto CreateNewTree(const KeyType &key, const ValueType &value) -> LeafPage *;
  auto CreateNewNonLeafTree(BPlusTreePage *L, BPlusTreePage *L_, const KeyType &key) -> page_id_t;
  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;
  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);
  template <typename T>
  auto Split(T *L) -> T *;
  // Insert a key-value pair into this leaf.
  auto InsertInLeaf(Page *node, const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;

  /// Insert a key which points to L_ into the parent of L.
  /// \return a pointer to the parent of L
  void InsertInParent(BPlusTreePage *L, const KeyType &key, BPlusTreePage *L_, Transaction *transaction);
  // Find the leaf node contains the key
  auto FindLeaf(const KeyType &key, Transaction *transaction, enum OPERATION opt) -> Page *;
  auto FindLeftMostLeaf() -> LeafPage *;
  /// Find the parent of node
  /// \param node
  /// \return the parent of node which is a internal page
  auto Parent(const BPlusTreePage *node) -> InternalPage *;
  // delete key from 'p'
  // returns the parent page_id of the calling function
  auto DeleteEntryLeaf(LeafPage *N, const KeyType &key, Transaction *transaction) -> page_id_t;
  auto DeleteEntryInternal(InternalPage *N, const KeyType &key, Transaction *transaction) -> page_id_t;
  void RedistributeLeaf(LeafPage *N, LeafPage *N_, KeyType K_);
  void RedistributeInternalPage(InternalPage *N, InternalPage *N_, KeyType K_);
  auto MergeLeaf(LeafPage *N, LeafPage *N_, KeyType K_, Transaction *transaction) -> LeafPage *;
  auto MergeInternalPage(InternalPage *N, InternalPage *N_, KeyType K_, Transaction *transaction) -> InternalPage *;
  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;
  void SetRootPageId(page_id_t page_id);
  auto RightSibling(LeafPage *node) -> LeafPage *;
  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  // for leaf node
  void InsertSorted(std::vector<KeyType> &keys, std::vector<ValueType> &values, const KeyType &key,
                    const ValueType &value);
  // for internal node
  void InsertSorted(std::vector<KeyType> &keys, std::vector<page_id_t> &values, const KeyType &key, page_id_t value);
  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);
  auto LeftSibling(InternalPage *node) -> InternalPage *;
  auto RightSibling(InternalPage *node) -> InternalPage *;
  // return the key in the parent of 'node' that point to 'node'
  auto KeyTo(InternalPage *node) -> KeyType;
  auto LeftSibling(LeafPage *node) -> LeafPage *;
  // return the key in the parent of 'node' that point to 'node'
  auto KeyTo(LeafPage *node) -> KeyType;
  // replace k1 to k2 in 'node'
  void ReplaceKey(InternalPage *node, KeyType k1, KeyType k2);
  auto IsSafe(Page *p, enum OPERATION operation) -> bool;
  void ReleaseLatches(Transaction *transaction);
  ReaderWriterLatch global_latch_;

 private:
  void UpdateRootPageId(int insert_record = 0);
  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;
  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_{INVALID_PAGE_ID};
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  INDEXITERATOR_TYPE *end_iterator_;
  void ReassignParentLinks(InternalPage *p_node, InternalPage *p_prime_node);
};

}  // namespace bustub
