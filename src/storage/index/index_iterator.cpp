/**
 * index_iterator.cpp
 */
#include <cassert>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, const page_id_t &cur_page_id)
    : bpm_(bpm), cur_page_id_(cur_page_id), index_(0) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { bpm_ = nullptr; };

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  BUSTUB_ASSERT(cur_page_id_ != INVALID_PAGE_ID, "current page id is INVALID_PAGE_ID");

  auto cur_page_guard = bpm_->FetchPageRead(cur_page_id_);
  auto cur_page = cur_page_guard.As<LeafPage>();

  if (cur_page->GetNextPageId() == INVALID_PAGE_ID && index_ >= cur_page->GetMaxSize()) {
    index_ = cur_page->GetMaxSize();
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  BUSTUB_ASSERT(cur_page_id_ != INVALID_PAGE_ID, "current page id is INVALID_PAGE_ID");

  auto cur_page_guard = bpm_->FetchPageRead(cur_page_id_);
  auto cur_page = cur_page_guard.As<LeafPage>();

  return cur_page->ValueAt(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(cur_page_id_ != INVALID_PAGE_ID, "current page id is INVALID_PAGE_ID");

  auto cur_page_guard = bpm_->FetchPageRead(cur_page_id_);
  auto cur_page = cur_page_guard.As<LeafPage>();

  // current iterator is already point to end;
  if (cur_page->GetNextPageId() == INVALID_PAGE_ID) {
    index_ = std::min(index_ + 1, cur_page->GetMaxSize());
  } else {
    if (index_ == cur_page->GetMaxSize() - 1) {  // the last element in the middle page
      index_ = 0;
      pre_page_id_ = cur_page_id_;
      cur_page_id_ = cur_page->GetNextPageId();
    } else {
      index_++;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return cur_page_id_ == itr.cur_page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool { return *this == itr; }

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
