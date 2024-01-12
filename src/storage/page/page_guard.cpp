#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;

  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }

  auto page_id = page_->GetPageId();
  if (page_id != INVALID_PAGE_ID) {
    bpm_->UnpinPage(page_id, is_dirty_);
  }

  page_ = nullptr;
  is_dirty_ = false;
  bpm_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this == &that) {
    return *this;
  }

  // drop the current guarded page
  Drop();

  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;

  // distroy the old page guard
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard() {  // NOLINT
  Drop();
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }

  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }

  // move, whatever the old page guard is null or not
  guard_ = std::move(that.guard_);

  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->RUnlatch();
  guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }

  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }

  // move, whatever the old page guard is null or not
  guard_ = std::move(that.guard_);

  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ == nullptr) {
    return;
  }

  guard_.page_->WUnlatch();
  guard_.Drop();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
