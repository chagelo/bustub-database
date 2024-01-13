//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Exist(const KeyType &key, ValueType &value, int *index,
                                       const KeyComparator &keycomp) const -> bool {
  int target_in_array = GetIndex(key, keycomp);

  if (target_in_array == GetSize() || keycomp(array_[target_in_array].first, key) != 0) {
    *index = target_in_array;
    return false;
  }

  *index = target_in_array;
  value = array_[target_in_array].second;
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndex(const KeyType &key, const KeyComparator &keycomp) const -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key,
                                 [&keycomp](const auto &pair, auto k) { return keycomp(pair.first, k) < 0; });
  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &keycomp)
    -> bool {
  if (GetSize() == GetMaxSize()) {
    return false;
  }

  auto index = GetIndex(key, keycomp);

  if (index == GetSize()) {
    IncreaseSize(1);
    *(array_ + index) = {key, value};
    return true;
  }

  // repeat key do nothing
  if (keycomp(array_[index].first, key) == 0) {
    return false;
  }

  IncreaseSize(1);
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_ + index) = {key, value};
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *right_page, const int &st, const int &des_st,
                                            const int &n) {
  right_page->CopyHalf(array_ + st, n, des_st);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalf(MappingType *src_array, const int &n, const int &des_st) {
  std::copy(src_array, src_array + n, array_ + des_st);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Move(const int &st, const int &direc) {
  if (direc == 1) {  // [....] -> [...|...]
    std::copy_backward(array_, array_ + GetSize(), array_ + st);
  } else {  // [...|...] <- [....]
    std::copy(array_ + st, array_ + GetSize(), array_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetBound(const int &idx, const int &size, bool &insert_left) -> int {
  int bound = idx;
  if (idx <= (size - 1) / 2) {
    bound = (size - 1) / 2;
    insert_left = true;
  } else {
    bound = (size + 1) / 2;
    insert_left = false;
  }
  return bound;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAt(const int &idx) {
  std::copy(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
