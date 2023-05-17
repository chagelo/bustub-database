#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  if (that.page_ != nullptr) {
    auto page_id = that.page_->GetPageId();
    auto is_dirty = that.page_->IsDirty();

    that.bpm_->UnpinPage(page_id, is_dirty);
  }
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
    auto is_dirty = page_->IsDirty();

    // is unpin fail, the page is not in buffer or pin count is 0
    bpm_->UnpinPage(page_id, is_dirty);
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

  if (that.page_ != nullptr) {
    auto page_id = that.page_->GetPageId();
    auto is_dirty = that.page_->IsDirty();

    that.bpm_->UnpinPage(page_id, is_dirty);
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
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  auto page = guard_.page_;
  guard_.Drop();
  page->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  auto page = guard_.page_;
  guard_.Drop();
  page->WUnlatch();
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
