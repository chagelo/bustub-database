#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

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
  if (page_ != nullptr) {
    auto page_id = page_->GetPageId();
    bpm_->UnpinPage(page_id, is_dirty_);
    page_ = nullptr;
    is_dirty_ = false;
    bpm_ = nullptr;
  }
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (page_ != nullptr) {
    auto page_id = page_->GetPageId();
    bpm_->UnpinPage(page_id, is_dirty_);
  }
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  bpm_ = that.bpm_;

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
  auto old_page = guard_.page_;
  auto old_page_id = old_page->GetPageId();
  auto new_page_id = that.PageId();
  guard_ = std::move(that.guard_);
  if (old_page != nullptr && old_page_id != new_page_id) {
    old_page->RUnlatch();
  }
  return *this;
}

void ReadPageGuard::Drop() {
  auto page = guard_.page_;
  guard_.Drop();
  if (page != nullptr) {
    page->RUnlatch();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  auto old_page = guard_.page_;
  auto old_page_id = old_page->GetPageId();
  auto new_page_id = that.PageId();
  guard_ = std::move(that.guard_);
  if (old_page != nullptr && old_page_id != new_page_id) {
    old_page->WUnlatch();
  }
  return *this;
}

void WritePageGuard::Drop() {
  auto page = guard_.page_;
  guard_.Drop();
  if (page != nullptr) {
    page->WUnlatch();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
