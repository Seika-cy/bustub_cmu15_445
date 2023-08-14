//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  auto lock = std::lock_guard<std::mutex>(latch_);
  *page_id = AllocatePage();
  Page *page = nullptr;
  frame_id_t frame_id = -1;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
    BUSTUB_ASSERT(page->page_id_ == INVALID_PAGE_ID, "The page id of free frame is not INVALID_PAGE_ID");
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(frame_id != -1, "Evict error");
    page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    page->ResetMemory();
    page_table_.erase(page->page_id_);
  } else {
    return nullptr;
  }
  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  page_table_.emplace(*page_id, frame_id);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  auto lock = std::lock_guard<std::mutex>(latch_);
  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_[page_id];
    page = &pages_[frame_id];
    ++(page->pin_count_);
    replacer_->RecordAccess(frame_id, access_type);
    return page;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
    BUSTUB_ASSERT(page->page_id_ == INVALID_PAGE_ID, "The page id of free frame is not INVALID_PAGE_ID");
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&frame_id);
    BUSTUB_ASSERT(frame_id != -1, "Evict error");
    page = &pages_[frame_id];
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
    disk_manager_->ReadPage(page_id, page->data_);
    page_table_.erase(page->page_id_);
  } else {
    return nullptr;
  }

  page->page_id_ = page_id;
  page->pin_count_ = 1;
  replacer_->RecordAccess(frame_id, access_type);
  page_table_.emplace(page_id, frame_id);
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  auto lock = std::lock_guard<std::mutex>(latch_);

  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  --(page->pin_count_);
  page->is_dirty_ = is_dirty;

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto lock = std::lock_guard<std::mutex>(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  BUSTUB_ASSERT(page_id == page->page_id_, page_id != page->page_id_);
  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  auto lock = std::lock_guard<std::mutex>(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    auto page = &pages_[i];
    if (page->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto lock = std::lock_guard<std::mutex>(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];

  if (page->pin_count_ != 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  page->ResetMemory();
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return {this, FetchPage(page_id, AccessType::Unknown)};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id, AccessType::Unknown);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id, AccessType::Unknown);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }
}  // namespace bustub
