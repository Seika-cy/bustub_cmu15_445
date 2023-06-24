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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();

  if (curr_size_ == 0) {
    return false;
  }

  struct TNode {
    frame_id_t fid_;
    size_t timestamp_;
    size_t k_;
  };

  std::vector<TNode> vec{};
  vec.reserve(curr_size_);

  for (const auto &[key, value] : node_store_) {
    if (value.is_evictable_) {
      TNode t = {
          .fid_ = value.fid_,
          .timestamp_ = (value.history_.empty()) ? 0 : value.history_.front(),
          .k_ = value.k_,
      };
      vec.emplace_back(t);
    }
  }

  BUSTUB_ASSERT(!vec.empty(), "");

  // less func of timestamp
  auto cmp = [this](TNode &lhs, TNode &rhs) -> bool {
    auto l = (lhs.k_ >= this->k_) ? lhs.timestamp_ : 0;
    auto r = (rhs.k_ >= this->k_) ? rhs.timestamp_ : 0;
    return (l != r) ? l < r : (lhs.timestamp_ < rhs.timestamp_);
  };

  // the minimum of timestamp is the maximux of k-distance.
  auto out = std::min_element(vec.begin(), vec.end(), cmp);

  *frame_id = out->fid_;

  auto &node = node_store_.at(*frame_id);
  node.history_.clear();
  node.k_ = 0;
  node.is_evictable_ = false;
  // node.is_evictable_ = false;
  // node_store_.erase(*frame_id);

  BUSTUB_ASSERT(curr_size_ > 0, "curr_size_ <=0");
  --curr_size_;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> lcok(latch_);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id].fid_ = frame_id;
    node_store_[frame_id].k_ = 0;
  }

  auto &node = node_store_.at(frame_id);

  BUSTUB_ASSERT(node.k_ == node.history_.size(), "");

  if (node.k_ == this->k_) {
    node.history_.pop_back();
  } else {
    ++(node.k_);
  }
  float n = 0.618;
  if (access_type == AccessType::Get) {
    node.history_.emplace_front(current_timestamp_ - n * 1000);
  } else if (access_type == AccessType::Scan) {
    node.history_.emplace_front(current_timestamp_ + n * 1000);
  } else {
    node.history_.emplace_front(current_timestamp_);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    node_store_[frame_id].fid_ = frame_id;
    node_store_[frame_id].k_ = 0;
  }

  auto &node = node_store_.at(frame_id);

  if (set_evictable ^ node.is_evictable_) {
    node.is_evictable_ = set_evictable;
    if (set_evictable) {
      ++curr_size_;
      BUSTUB_ASSERT(curr_size_ <= replacer_size_, "");
    } else {
      BUSTUB_ASSERT(curr_size_ > 0, "");
      --curr_size_;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  current_timestamp_ = std::chrono::system_clock::now().time_since_epoch().count();
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "frame id is invalid");

  if (node_store_.count(frame_id) == 0) {
    return;
  }

  auto &node = node_store_.at(frame_id);

  if (node.is_evictable_) {
    node.history_.clear();
    node.k_ = 0;
    node.is_evictable_ = false;

    BUSTUB_ASSERT(curr_size_ > 0, "");
    --curr_size_;
  } else {
    throw ExecutionException("move a non-evictable frame");
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
