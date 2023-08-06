//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }
  // iterate in reverse order
  for (auto it = infinite_list_.rbegin(); it != infinite_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      infinite_list_.erase(infinite_map_[frame]);
      infinite_map_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }
  // iterate in reverse order: the first will be the least recently element
  for (auto it = k_backward_list_.rbegin(); it != k_backward_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      access_count_[frame] = 0;
      k_backward_list_.erase(k_backward_map_[frame]);
      k_backward_map_.erase(frame);
      *frame_id = frame;
      curr_size_--;
      is_evictable_[frame] = false;
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  access_count_[frame_id]++;

  // if the count is equal to k, then this is considered as a k-recently-used frame
  // we move it to the satisfy list: k_backward_list_
  if (access_count_[frame_id] == k_) {
    auto it = infinite_map_[frame_id];
    infinite_list_.erase(it);
    infinite_map_.erase(frame_id);

    k_backward_list_.push_front(frame_id);
    k_backward_map_[frame_id] = k_backward_list_.begin();
  } else if (access_count_[frame_id] > k_) {
    // it is already in the list, so just update the most recent access
    if (k_backward_map_.count(frame_id) != 0U) {
      auto it = k_backward_map_[frame_id];
      k_backward_list_.erase(it);
    }

    k_backward_list_.push_front(frame_id);
    k_backward_map_[frame_id] = k_backward_list_.begin();
  } else {
    // otherwise, just add the frame into the infinite_list_
    if (infinite_map_.count(frame_id) == 0U) {
      infinite_list_.push_front(frame_id);
      infinite_map_[frame_id] = infinite_list_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  if (access_count_[frame_id] == 0) {
    return;
  }

  if (!is_evictable_[frame_id] && set_evictable) {
    curr_size_++;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    curr_size_--;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }

  auto cnt = access_count_[frame_id];
  if (cnt == 0) {
    return;
  }
  if (!is_evictable_[frame_id]) {
    throw std::exception();
  }
  if (cnt < k_) {
    infinite_list_.erase(infinite_map_[frame_id]);
    infinite_map_.erase(frame_id);

  } else {
    k_backward_list_.erase(k_backward_map_[frame_id]);
    k_backward_map_.erase(frame_id);
  }
  curr_size_--;
  access_count_[frame_id] = 0;
  is_evictable_[frame_id] = false;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
