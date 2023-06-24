#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::swap(this->bpm_, that.bpm_);
  std::swap(this->page_, that.page_);
  std::swap(this->is_dirty_, that.is_dirty_);
}

void BasicPageGuard::Drop() {
  bpm_->UnpinPage(PageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    std::swap(this->bpm_, that.bpm_);
    std::swap(this->page_, that.page_);
    std::swap(this->is_dirty_, that.is_dirty_);
  }
  return *this;
}
BasicPageGuard::~BasicPageGuard() {
  if (page_ != nullptr) {
    Drop();
  }
}  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_, AccessType::Scan);
  guard_.page_->RUnlatch();
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  guard_.bpm_->UnpinPage(PageId(), guard_.is_dirty_, AccessType::Get);
  guard_.page_->WUnlatch();
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
}

WritePageGuard::~WritePageGuard() {
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

}  // namespace bustub
