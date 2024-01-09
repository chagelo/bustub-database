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
#include <cstdlib>
#include <utility>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
// #include "googletest/googletest/include/gtest/gtest.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  rwlatch_ = new std::mutex[pool_size_];

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] rwlatch_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // assert page_id != nullptr
  // assert(page_id);

  std::scoped_lock<std::mutex> lock(latch_);
  // latch_.lock();
  // no free frame and all frames are in used
  if (free_list_.empty() && replacer_->Size() == 0) {
    // latch_.unlock();
    return nullptr;
  }

  frame_id_t free_frame = 0;
  auto next_page_id = AllocatePage();

  if (!free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();

  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&free_frame);
  }

  page_table_.insert({next_page_id, free_frame});
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);

  *page_id = next_page_id;
  auto &targ_page = pages_[free_frame];

  // initilize metadata
  // targ_page.WLatch();
  if (page_table_.find(targ_page.page_id_) != page_table_.end() && next_page_id != targ_page.page_id_) {
    page_table_.erase(targ_page.page_id_);
  }
  // latch_.unlock();

  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(targ_page.GetPageId(), targ_page.GetData());
  }

  // reset metadata
  targ_page.ResetMemory();
  targ_page.is_dirty_ = false;
  targ_page.pin_count_ = 1;
  targ_page.page_id_ = next_page_id;
  // targ_page.WUnlatch();
  return pages_ + free_frame;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  // find the target page by page_id
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    auto &page = pages_[frame_id];
    page.pin_count_++;
    latch_.unlock();
    // if (read == 1) {
    //   page.RLatch();
    // } else if (read == 0) {
    //   page.WLatch();
    // }
    return pages_ + frame_id;
  }

  // no free frame and all frames are in used
  if (free_list_.empty() && replacer_->Size() == 0) {
    latch_.unlock();
    return nullptr;
  }

  frame_id_t free_frame = 0;

  // get a free frame from lru-k or evict a frame

  if (!free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();

    // ASSERT_EQ(page_table_.find(next_page_id), page_table_.end());
  } else if (replacer_->Size() > 0) {
    replacer_->Evict(&free_frame);

    // ASSERT_EQ(true, replacer_->Evict(&free_frame));
  }

  // modify the bpm data structures
  page_table_.insert({page_id, free_frame});
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);

  auto &targ_page = pages_[free_frame];

  if (page_table_.find(targ_page.page_id_) != page_table_.end() && page_id != targ_page.page_id_) {
    page_table_.erase(targ_page.page_id_);
  }

  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(targ_page.GetPageId(), targ_page.GetData());
  }

  // read the missing page from disk
  disk_manager_->ReadPage(page_id, targ_page.data_);

  // reset metadata
  // comment targ_page.ResetMemory(), just overwrite the old data
  targ_page.is_dirty_ = false;
  targ_page.pin_count_ = 1;
  targ_page.page_id_ = page_id;

  latch_.unlock();
  // if (read == 1) {
  //   targ_page.RLatch();
  // } else if (read == 0) {
  //   targ_page.WLatch();
  // }

  return pages_ + free_frame;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // latch_.lock();
  std::scoped_lock<std::mutex> lock(latch_);
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return false;
  }
  auto &targ_page = pages_[targ_iter->second];
  // targ_page.WLatch();

  if (targ_page.pin_count_ == 0) {
    // targ_page.WUnlatch();
    // latch_.unlock();
    return false;
  }

  // pin_count decrease to 0, set lru-k frame to be evictable
  if (--targ_page.pin_count_ == 0) {
    replacer_->SetEvictable(targ_iter->second, true);
  }
  // latch_.unlock();

  // origin page is clean, then up above do some modify
  targ_page.is_dirty_ |= is_dirty;
  // targ_page.WUnlatch();
  return true;
}

auto BufferPoolManager::UnpinPage(Page *page) -> bool {
  if (page == nullptr) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  // latch_.lock();
  // page->WLatch();

  auto &page_id = page->page_id_;
  auto &pin_count = page->pin_count_;
  if (page_id == INVALID_PAGE_ID || pin_count == 0 || page_table_.find(page_id) == page_table_.end()) {
    // latch_.unlock();
    // page->WUnlatch();
    return false;
  }
  auto &frame_id = page_table_[page_id];
  if (--page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, false);
  }

  // page->WUnlatch();
  // latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  // latch_.lock();
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return false;
  }
  auto &targ_page = pages_[targ_iter->second];

  // bfm latch unlock after accessing the latch of the page
  // targ_page.WLatch();
  // latch_.unlock();

  // write page to disk
  disk_manager_->WritePage(page_id, targ_page.GetData());

  targ_page.is_dirty_ = false;
  // targ_page.WUnlatch();
  return true;
}

auto BufferPoolManager::FlushPageWithoutLatch(page_id_t page_id, frame_id_t frame_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto &targ_page = pages_[frame_id];

  // write page to disk
  disk_manager_->WritePage(page_id, targ_page.GetData());

  targ_page.is_dirty_ = false;
  return false;
}

void BufferPoolManager::FlushAllPages() {
  // latch_.lock();
  std::scoped_lock<std::mutex> lock(latch_);
  /*
   * here use page_ref to store the pointer points to the page(what to write to disk)
   * and the page_id in disk(where to write in disk),
   * when unlock the buffer latch, another thread might modify the
   * page_table_, so here wo store the pointer first.
   */
  std::vector<std::pair<page_id_t, frame_id_t>> page_ref;
  for (auto [page_id, frame_id] : page_table_) {
    // pages_[frame_id].WLatch();
    page_ref.emplace_back(std::make_pair(page_id, frame_id));
  }
  // latch_.unlock();
  for (auto [page_id, frame_id] : page_ref) {
    FlushPageWithoutLatch(page_id, frame_id);
    // pages_[frame_id].WUnlatch();
  }
  // latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // latch_.lock();
  std::scoped_lock<std::mutex> lock(latch_);
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return true;
  }

  auto frame_id = targ_iter->second;
  auto &targ_page = pages_[frame_id];

  // read lock for page
  // targ_page.WLatch();
  // assert(targ_page.page_id_ == page_id);

  if (targ_page.pin_count_ > 0) {
    // targ_page.WUnlatch();
    // latch_.unlock();
    return false;
  }

  // update shared datastructures, include page_tabel_
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(targ_iter);
  // latch_.unlock();

  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(page_id, targ_page.GetData());
  }

  // reset metadata and memory
  targ_page.page_id_ = INVALID_PAGE_ID;
  targ_page.is_dirty_ = false;
  targ_page.ResetMemory();
  // targ_page.WUnlatch();

  DeallocatePage(page_id);
  // latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id, AccessType::Unknown);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id, AccessType::Unknown);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
