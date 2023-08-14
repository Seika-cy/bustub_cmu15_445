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
#include "common/exception.h"

namespace bustub {
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (curr_size_ == 0) {
    return false;
  }

  LRUKNode *t = nullptr;
  if (!lru_list_.empty()) {
    t = lru_list_.back();
    lru_list_.pop_back();

  } else {
    t = k_list_.back();
    k_list_.pop_back();
  }

  *frame_id = t->fid_;
  --curr_size_;
  t->is_evictable_ = false;
  t->history_.clear();
  t->k_ = 0;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode(frame_id);
  }
  LRUKNode *node = &node_store_.at(frame_id);

  BUSTUB_ASSERT(node->k_ <= this->k_, "node->k_ > this->k_");
  BUSTUB_ASSERT(node->k_ == node->history_.size(), "node.k_ != node.history_.size()");

  if (node->k_ == this->k_) {
    node->history_.pop_back();
    node->history_.push_front(current_timestamp_);
    if (node->is_evictable_) {
      BUSTUB_ASSERT(dict_.count(frame_id), "This frame does not exist in dict");
      k_list_.splice(k_list_.begin(), k_list_, dict_[frame_id]);
    }
  } else {
    ++(node->k_);
    node->history_.push_front(current_timestamp_);
    if (node->is_evictable_) {
      if (node->k_ == this->k_) {
        BUSTUB_ASSERT(dict_.count(frame_id), "This frame does not exist in dict");
        k_list_.splice(k_list_.begin(), lru_list_, dict_[frame_id]);
      } else if (dict_.count(frame_id) == 0) {
        lru_list_.push_front(node);
        dict_[frame_id] = lru_list_.begin();
      }
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id] = LRUKNode(frame_id);
  }
  LRUKNode *node = &node_store_.at(frame_id);

  // if set_evictable is modified
  if (set_evictable ^ node->is_evictable_) {
    node->is_evictable_ = set_evictable;

    if (set_evictable) {
      if (node->k_ == this->k_) {
        k_list_.push_front(node);
        auto cmp = [](LRUKNode *lhs, LRUKNode *rhs) { return lhs->history_.front() > rhs->history_.front(); };
        // TODO(cyrus): 基本有序序列排序，可以优化到 O(n)
        k_list_.sort(cmp);
      } else {
        lru_list_.push_front(node);
        dict_[frame_id] = lru_list_.begin();
        auto cmp = [](LRUKNode *lhs, LRUKNode *rhs) { return lhs->history_.back() > rhs->history_.back(); };
        lru_list_.sort(cmp);
      }

      ++curr_size_;

    } else {
      BUSTUB_ASSERT(dict_.count(frame_id), "This frame does not exist in dict");

      if (node->k_ == this->k_) {
        k_list_.erase(dict_[frame_id]);
        dict_.erase(frame_id);
      } else {
        lru_list_.erase(dict_[frame_id]);
        dict_.erase(frame_id);
      }

      --curr_size_;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    return;
  }
  LRUKNode *node = &node_store_.at(frame_id);

  if (node->is_evictable_) {
    BUSTUB_ASSERT(dict_.count(frame_id), "This frame does not exist in dict");
    if (node->k_ == this->k_) {
      k_list_.erase(dict_[frame_id]);
      dict_.erase(frame_id);
    } else {
      lru_list_.erase(dict_[frame_id]);
      dict_.erase(frame_id);
    }

    --curr_size_;
    node->history_.clear();
    node->k_ = 0;
    node->is_evictable_ = false;

  } else {
    throw ExecutionException("remove a non-evictable frame");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
