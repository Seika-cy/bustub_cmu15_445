#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::swap(bpm_, that.bpm_);
  std::swap(page_, that.page_);
  std::swap(is_dirty_, that.is_dirty_);
}

void BasicPageGuard::Drop() {
  is_dirty_ = false;
  if (page_ == nullptr && bpm_ == nullptr) {
    return;
  }
  BUSTUB_ASSERT(page_ != nullptr && bpm_ != nullptr, "only one of page and bpm is null");
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    this->Drop();
    std::swap(bpm_, that.bpm_);
    std::swap(page_, that.page_);
    std::swap(is_dirty_, that.is_dirty_);
  }
  return *this;
}
BasicPageGuard::~BasicPageGuard() { Drop(); }

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->RUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->WUnlatch();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
