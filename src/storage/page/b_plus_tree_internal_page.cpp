//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <iterator>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertIndex(const KeyType &key, const KeyComparator &keycomp, bool isRoot) -> int {
  int err = static_cast<int>(isRoot);
  auto target = std::lower_bound(array_ + err, array_ + GetSize(), key,
                                 [&keycomp](const auto &pair, auto k) { return keycomp(pair.first, k) < 0; });
  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const page_id_t &page_id, const KeyComparator &keycomp,
                                            bool isRoot) -> bool {
  if (GetSize() == GetMaxSize()) {
    return false;
  }

  auto index = InsertIndex(key, keycomp, isRoot);

  if (index == GetSize()) {
    IncreaseSize(1);
    *(array_ + index) = {key, page_id};
    return true;
  }

  // repeat key do nothing
  if (keycomp(array_[index].first, key) == 0) {
    return false;
  }

  IncreaseSize(1);
  std::move_backward(array_ + index, array_ + GetSize(), array_ + GetSize() + 1);
  *(array_ + index) = {key, page_id};
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindChild(const KeyType &key, int *idx, const KeyComparator &keycomp) const
    -> ValueType {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&keycomp](const auto &pair, auto k) { return keycomp(pair.first, k) < 0; });

  // 所有的 key 都比 target 小是不可能的，因为搜索是递归进行的
  // the key large than all key in this internal node, just return the last page
  if (target == array_ + GetSize()) {
    *idx = GetSize() - 1;
    return ValueAt(GetSize() - 1);
  }

  // given key is the minimum element int the subtreee
  if (keycomp(target->first, key) == 0) {
    *idx = std::distance(array_, target);
    return target->second;
  }

  // target large than the key
  *idx = std::distance(array_, target) - 1;
  return std::prev(target)->second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *right_page, const int &st, const int &des_st,
                                                const int &n) {
  right_page->CopyHalf(array_ + st, n, des_st);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalf(MappingType *src_array, const int &n, const int &des_st) {
  std::copy(src_array, src_array + n, array_ + des_st);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Move(const int &st, const int &direc) {
  if (direc == 1) {  // [....] -> [...|...]
    std::copy_backward(array_, array_ + GetSize(), array_ + st);
  } else {  // [...|...] <- [....]
    std::copy(array_ + st, array_ + GetSize(), array_);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RootInit(const page_id_t &page_id_1, const KeyType &key,
                                              const page_id_t &page_id_2) {
  *array_ = {KeyType(), page_id_1};
  *(array_ + 1) = {key, page_id_2};
  SetSize(2);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetBound(const int &idx, const int &size, bool &insert_left) -> int {
  int bound = idx;
  if (idx <= size / 2) {
    bound = size / 2;
    insert_left = true;
  } else {
    bound = size / 2 + 1;
    insert_left = false;
  }
  return bound;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAt(const int &idx) {
  std::copy(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
