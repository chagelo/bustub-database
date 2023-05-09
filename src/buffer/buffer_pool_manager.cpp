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

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "googletest/googletest/include/gtest/gtest.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {

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
  // assert page_id != nullptr
  assert(page_id);

  latch_.lock();
  // no free frame and all frames are in used 
  if (!free_list_.empty() && replacer_->Size() == 0) {
    latch_.unlock();
    return nullptr;
  }

  frame_id_t free_frame = 0;
  auto next_page_id = AllocatePage();

  if (free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();

    // ASSERT_EQ(page_table_.find(next_page_id), page_table_.end());
  } 

  else if (replacer_->Size() > 0) {
    replacer_->Evict(&free_frame)
    //ASSERT_EQ(true, replacer_->Evict(&free_frame));
  }
  
  page_table_.insert({next_page_id, free_frame});
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);

  *page_id = next_page_id;
  auto &targ_page = pages_[free_frame];

  // initilize metadata
  targ_page.rwlatch_.WLock();
  if (page_table_.find(targ_page.page_id_) != page_table_.end() && next_page_id != targ_page.page_id_) {
    page_table_.erase(targ_page.page_id_);
  }
  latch_.unlock();
  
  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(targ_page.GetPageId(), targ_page.GetData());
  }

  // reset metadata
  targ_page.ResetMemory();
  targ_page.is_dirty_ = false;
  targ_page.pin_count_ = 1;
  targ_page.page_id_ = next_page_id;
  targ_page.rwlatch_.WUnlock();

  return pages_ + free_frame;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  
  latch_.lock();

  // find the target page by page_id
  if (page_table_.find(page_id) != page_table_.end()) {
    auto frame_id = page_table_[page_id];
    latch_.unlock();
    return pages_ + frame_id;
  }

  // no free frame and all frames are in used 
  if (!free_list_.empty() && replacer_->Size() == 0) {
    latch_.unlock();
    return nullptr;
  }

  frame_id_t free_frame = 0;
  auto next_page_id = AllocatePage();

  // get a free frame from lru-k or evict a frame

  if (free_list_.empty()) {
    free_frame = free_list_.front();
    free_list_.pop_front();

    // ASSERT_EQ(page_table_.find(next_page_id), page_table_.end());
  } 

  else if (replacer_->Size() > 0) {
    replacer_->Evict(&free_frame);

    // ASSERT_EQ(true, replacer_->Evict(&free_frame));
  }
  
  // modify the bpm data structures
  page_table_.insert({next_page_id, free_frame});
  replacer_->RecordAccess(free_frame);
  replacer_->SetEvictable(free_frame, false);

  auto &targ_page = pages_[free_frame];

  // initilize metadata
  targ_page.rwlatch_.WLock();
  if (page_table_.find(targ_page.page_id_) != page_table_.end() && next_page_id != targ_page.page_id_) {
    page_table_.erase(targ_page.page_id_);
  }
  latch_.unlock();
  
  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(targ_page.GetPageId(), targ_page.GetData());
  }

  // read the missing page from disk
  disk_manager_->ReadPage(next_page_id, targ_page.data_);

  // reset metadata
  // targ_page.ResetMemory();
  targ_page.is_dirty_ = false;
  targ_page.pin_count_ = 1;
  targ_page.page_id_ = next_page_id;
  targ_page.rwlatch_.WUnlock();

  return pages_ + free_frame;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return false;
  }
  auto &targ_page = pages_[targ_iter->second];
  targ_page.WLatch();

  if (targ_page.pin_count_ == 0) {
    targ_page.WUnlatch();
    latch_.unlock();
    return false;
  }

  // pin_count decrease to 0, set lru-k frame to be evictable
  if (--targ_page.pin_count_ == 0) {
    replacer_->SetEvictable(targ_iter->second, false);
  }
  latch_.unlock();

  // origin page is clean, then up above do some modify
  if (!targ_page.is_dirty_ && is_dirty) {
    targ_page.is_dirty_ = is_dirty;
  }

  targ_page.WUnlatch();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  latch_.lock();
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return false;
  }
  auto &targ_page = pages_[targ_iter->second];

  // bfm latch unlock after accessing the latch of the page 
  targ_page.rwlatch_.WLock();
  latch_.unlock();

  // write page to disk
  disk_manager_->WritePage(page_id, targ_page.GetData());

  targ_page.is_dirty_ = false;
  targ_page.rwlatch_.WUnlock();

  return false; 
}

auto BufferPoolManager::FlushPageWithoutGloablLatch(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  
  auto targ_iter = page_table_.find(page_id);
  if (targ_iter == page_table_.end()) {
    return false;
  }
  auto &targ_page = pages_[targ_iter->second];

  // bfm latch unlock after accessing the latch of the page 
  targ_page.rwlatch_.WLock();

  // write page to disk
  disk_manager_->WritePage(page_id, targ_page.GetData());

  targ_page.is_dirty_ = false;
  targ_page.rwlatch_.WUnlock();

  return false; 
}

// TODO(ready to modify)
void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto [page_id, _]: page_table_) {
    FlushPageWithoutGloablLatch(page_id);
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto targ_iter = page_table_.find(page_id);  
  if (targ_iter == page_table_.end()) {
    return true; 
  }

  auto frame_id = targ_iter->second;
  auto &targ_page = pages_[frame_id];

  // read lock for page
  targ_page.rwlatch_.WLock();
  assert(targ_page.page_id_ == page_id);

  if (targ_page.pin_count_ > 0) {
    targ_page.rwlatch_.WUnlock();
    latch_.lock();
    return false;
  }

  // update shared datastructures, include page_tabel_
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_.erase(targ_iter);
  latch_.unlock();

  if (targ_page.is_dirty_) {
    disk_manager_->WritePage(page_id, targ_page.GetData());
  }

  // reset metadata and memory
  targ_page.page_id_ = INVALID_PAGE_ID;
  targ_page.is_dirty_ = false;
  targ_page.ResetMemory();
  targ_page.rwlatch_.WUnlock();

  DeallocatePage(page_id);

  return true;
}

void BufferPoolManager::ResetPage(Page &page, ) {
  page.page_id_ = INVALID_PAGE_ID;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
