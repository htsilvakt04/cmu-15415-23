//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t frame_id = -1;

  // this if-elseif is try to get the proper frame_id
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {  // evic some frame, to get a new one
    if (pages_[frame_id].IsDirty()) {
      // write back the content to the disk
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    pages_[frame_id].ResetMemory();
    // remove the mapping
    page_table_->Remove(pages_[frame_id].GetPageId());
  }

  if (frame_id == -1) {
    return nullptr;
  }
  // set the page_id
  *page_id = AllocatePage();

  // store the mapping into the page_table
  page_table_->Insert(*page_id, frame_id);
  // set the page_id
  pages_[frame_id].page_id_ = *page_id;
  // reset the new page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // record access in the replacer
  replacer_->RecordAccess(frame_id);
  // pin it
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
 * but all frames are currently in use and not evictable (in another word, pinned).
 *
 * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
 * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
 * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
 * to disk and update the metadata of the new page
 *
 * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
 *
 * @param page_id id of page to be fetched
 * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
 */
auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t frame_id = -1;

  if (page_table_->Find(page_id, frame_id)) {
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {  // evic some frame x, to get a new one
    page_id_t evicted_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      // write back the content to the disk
      disk_manager_->WritePage(evicted_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }

    // remove the mapping
    page_table_->Remove(evicted_page_id);
  }

  if (frame_id == -1) {
    return nullptr;
  }

  // store the mapping into the page_table
  page_table_->Insert(page_id, frame_id);
  // store the new page_id to the Page object
  pages_[frame_id].page_id_ = page_id;
  // reset the new page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  // record access in the replacer
  replacer_->RecordAccess(frame_id);
  // pin it
  replacer_->SetEvictable(frame_id, false);

  // fetch from disk
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  return &pages_[frame_id];
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
 * 0, return false.
 *
 * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
 * Also, set the dirty flag on the page to indicate if the page was modified.
 *
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
 */
auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() <= 0) {
    return false;
  }

  // Decrement the pin count
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].GetPinCount() == 0) {
    // If the pin count reaches 0, the frame should be evictable by the replacer
    replacer_->SetEvictable(frame_id, true);
  }

  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  return true;
}
auto BufferPoolManagerInstance::GetPinCount(page_id_t page_id) -> int {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPinCount() <= 0) {
    return -1;
  }

  return pages_[frame_id].GetPinCount();
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  frame_id_t frame_id;
  std::lock_guard<std::mutex> l(latch_);
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPageId() == INVALID_PAGE_ID) {
    return false;
  }

  // flush to disk
  disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  // unset the dirty flag
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; ++i) {
    FlushPgImp(pages_[i].GetPageId());
  }
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
 * page is pinned and cannot be deleted, return false immediately.
 *
 * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
 * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
 * imitate freeing the page on the disk.
 *
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> l(latch_);
  frame_id_t frame_id;

  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].GetPageId() == INVALID_PAGE_ID) {
    return true;
  }

  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  // remove the entry in the page-table
  page_table_->Remove(page_id);
  // remove from the replacer
  replacer_->Remove(frame_id);
  // add back the frame_id to the free_list_
  free_list_.push_back(frame_id);
  // invalid the page
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;

  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
