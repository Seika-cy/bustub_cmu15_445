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
  // TODO(students): remove this line after you have implemented the buffer pool manager
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

auto BufferPoolManager::GetPage(page_id_t page_id) -> Page * {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      return &pages_[i];
    }
  }
  return nullptr;
}

auto BufferPoolManager::GetPageId(frame_id_t fid) -> page_id_t {
  for (const auto &[key, value] : page_table_) {
    if (value == fid) {
      auto ret = key;
      page_table_.erase(key);
      return ret;
    }
  }
  return INVALID_PAGE_ID;
}

auto BufferPoolManager::GetFreePage() -> Page * {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == INVALID_PAGE_ID) {
      return &pages_[i];
    }
  }
  return nullptr;
}

auto BufferPoolManager::WriteBackPage(page_id_t page_id) -> Page * {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() == page_id) {
      if (pages_[i].IsDirty()) {
        pages_[i].RLatch();
        disk_manager_->WritePage(page_id, pages_[i].GetData());
        pages_[i].RUnlatch();
      }
      return &pages_[i];
    }
  }
  return nullptr;
}

auto BufferPoolManager::AllocFrame(page_id_t page_id) -> Page * {
  frame_id_t frame_id = -1;
  Page *ret = nullptr;

  if (free_list_.empty() && !replacer_->Evict(&frame_id)) {
    return nullptr;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    BUSTUB_ASSERT(frame_id != -1, "don't find frame");

    ret = GetFreePage();
  } else {
    BUSTUB_ASSERT(frame_id != -1, "don't find frame");

    auto old_page_id = GetPageId(frame_id);
    BUSTUB_ASSERT(old_page_id != INVALID_PAGE_ID, "don't find old page");
    ret = WriteBackPage(old_page_id);

    page_table_.erase(old_page_id);
  }

  BUSTUB_ASSERT(ret != nullptr, "don't find page");
  ret->page_id_ = page_id;
  ret->is_dirty_ = false;
  ret->pin_count_ = 1;
  ret->WLatch();
  ret->ResetMemory();
  ret->WUnlatch();
  
  
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_.emplace(page_id, frame_id);

  return ret;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  // std::unique_lock<std::mutex> lcok(latch_);
  *page_id = AllocatePage();
  return AllocFrame(*page_id);
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = -1;
  Page *page = nullptr;

  if (page_table_.count(page_id) != 0) {
    frame_id = page_table_.at(page_id);
    page = GetPage(page_id);
  } else {
    page = AllocFrame(page_id);

    if (page == nullptr) {
      return nullptr;
    }

    frame_id = page_table_.at(page_id);
    BUSTUB_ASSERT(frame_id != -1, "don't find frame");

    page->WLatch();
    disk_manager_->ReadPage(page_id, page->data_);
    page->WUnlatch();
  }
  replacer_->RecordAccess(frame_id);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto page = GetPage(page_id);
  auto frame_id = page_table_.at(page_id);
  bool flag = false;
  BUSTUB_ASSERT(page != nullptr, "");
  if (page->GetPinCount() == 0) {
    return false;
  }

  --(page->pin_count_);
  if (page->GetPinCount() == 0) {
    flag = true;
  }
  page->is_dirty_ = is_dirty;
  if (flag) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  auto page = GetPage(page_id);
  if (page == nullptr) {
    return false;
  }

  page->RLatch();
  disk_manager_->WritePage(page_id, page->GetData());
  page->RUnlatch();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    auto &page = pages_[i];

    if (page.GetPageId() != INVALID_PAGE_ID) {
      page.RLatch();
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
      page.RUnlatch();
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (page_table_.count(page_id) == 0) {
    return true;
  }

  auto page = GetPage(page_id);

  if (page->GetPinCount() != 0) {
    return false;
  }

  if (page->IsDirty()) {
    page->RLatch();
    disk_manager_->WritePage(page_id, page->data_);
    page->RUnlatch();
    page->is_dirty_ = false;
  }
  page->WLatch();
  page->ResetMemory();
  page->WUnlatch();
  page->page_id_ = INVALID_PAGE_ID;

  auto frame_id = page_table_.at(page_id);
  free_list_.emplace_back(frame_id);

  page_table_.erase(page_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }

  ++(page->pin_count_);

  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  ++(page->pin_count_);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
