#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto head_page = guard.AsMut<BPlusTreeHeaderPage>();
  head_page->root_page_id_ = INVALID_PAGE_ID;
  root_page_id_ = INVALID_PAGE_ID;
  leaf_max_size_ = std::min(leaf_max_size_, internal_max_size_ - 1);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  Context ctx;
  root_page_id_latch_.RLock();
  if (IsEmpty()) {
    root_page_id_latch_.RUnlock();
    return false;
  }

  auto targ_leaf_page_gurad = GetLeaf(ctx, key);
  auto leaf_node = targ_leaf_page_gurad.template As<LeafPage>();

  ValueType val;
  int idx;
  auto existed = leaf_node->Exist(key, val, &idx, comparator_);

  if (!existed) {
    // root_page_id_latch_.RUnlock();
    // ReleaseHeader(ctx);
    return false;
  }

  result->push_back(val);
  // root_page_id_latch_.RUnlock();
  // ReleaseHeader(ctx);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  Context ctx;
  FetchHeaderWrite(ctx);

  root_page_id_latch_.WLock();
  ctx.root_page_id_ = root_page_id_;
  if (IsEmpty()) {
    NewRootPage(ctx, key, value);
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);
    return true;
  }

  GetLeafAndUpdate(ctx, key, nullptr);
  auto cur_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto cur_page = cur_guard.template AsMut<LeafPage>();

  // insert a exist key
  ValueType temp;
  int idx;
  if (cur_page->Exist(key, temp, &idx, comparator_)) {
    ctx.write_set_.clear();
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);
    return false;
  }

  // not full, insert directly
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {
    ctx.write_set_.clear();
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);

    auto ok = cur_page->Insert(key, value, comparator_);
    return ok;
  }

  // insert into leaf
  bool insert_left = true;
  page_id_t right_page_id;
  auto right_bound = cur_page->GetBound(idx, cur_page->GetSize(), insert_left);
  auto right_page = Split(cur_page, right_bound, &right_page_id);

  if (insert_left) {
    cur_page->Insert(key, value, comparator_);
  } else {
    right_page->Insert(key, value, comparator_);
  }

  // leaf page, set next page id
  right_page->SetNextPageId(cur_page->GetNextPageId());
  cur_page->SetNextPageId(right_page_id);

  // the first key of right page taken to parent, (key, this page id)
  auto split_key = right_page->KeyAt(0);

  // BUSTUB_ASSERT(ctx_.write_set_.empty(), "error ctx deque empty");

  // insert into internal

  return InsertInternal(ctx, split_key, right_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternal(Context &ctx, KeyType key, page_id_t page_id) -> bool {
  int idx;
  WritePageGuard cur_guard;
  while (!ctx.write_set_.empty()) {
    cur_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto cur_page = cur_guard.AsMut<InternalPage>();

    // current page is not full, internal page, size == maxsize means full
    if (cur_page->GetSize() < cur_page->GetMaxSize()) {
      root_page_id_latch_.WUnlock();
      ctx.write_set_.clear();
      ReleaseHeader(ctx);

      auto ok = cur_page->Insert(key, page_id, comparator_, 1);

      return ok;
    }

    idx = cur_page->InsertIndex(key, comparator_, 0);

    // split current page
    bool insert_left = true;
    page_id_t right_page_id;
    auto right_bound = cur_page->GetBound(idx, cur_page->GetMaxSize(), insert_left);
    auto right_page = Split(cur_page, right_bound, &right_page_id);

    if (insert_left) {
      cur_page->Insert(key, page_id, comparator_, 1);
    } else {
      right_page->Insert(key, page_id, comparator_, 0);
    }

    // (key[0], right page's page_id) taken up to parent
    key = right_page->KeyAt(0);
    page_id = right_page_id;
  }

  // the split is continue, so the current node is root
  page_id_t new_root_page_id;
  auto _ = bpm_->NewPageGuarded(&new_root_page_id);

  auto old_root_page_id = root_page_id_;
  root_page_id_ = new_root_page_id;
  root_page_id_latch_.WUnlock();

  auto root_guard = bpm_->FetchPageWrite(new_root_page_id);
  _.Drop();

  auto root_page = root_guard.AsMut<InternalPage>();
  root_page->Init(internal_max_size_);
  root_page->RootInit(old_root_page_id, key, page_id);

  auto header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;
  ctx.root_page_id_ = new_root_page_id;
  ctx.write_set_.clear();
  ReleaseHeader(ctx);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *cur_page, const int &right_start, page_id_t *page_id) -> N * {
  // 新建一个空的page
  auto temp = bpm_->NewPageGuarded(page_id);
  auto right_page_guard = bpm_->FetchPageWrite(temp.PageId());
  temp.Drop();

  auto right_page = right_page_guard.AsMut<N>();

  int max_size = 0;
  if (cur_page->IsLeafPage()) {
    max_size = leaf_max_size_;
  } else {
    max_size = internal_max_size_;
  }

  right_page->Init(max_size);

  cur_page->MoveHalfTo(right_page, right_start, 0, cur_page->GetSize() - right_start);
  right_page->SetSize(cur_page->GetSize() - right_start);
  cur_page->SetSize(right_start);

  return right_page;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  Context ctx;
  FetchHeaderWrite(ctx);

  // empty tree
  root_page_id_latch_.WLock();
  // ctx.root_page_id_ = root_page_id_;
  if (IsEmpty()) {
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);
    return;
  }

  std::unordered_map<page_id_t, int> pos;
  GetLeafAndUpdate(ctx, key, &pos);

  // if not exist the key
  ValueType val;
  int idx;
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  if (!leaf_page->Exist(key, val, &idx, comparator_)) {
    ctx.write_set_.clear();
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);
    return;
  }

  leaf_page->RemoveAt(idx);

  // after delete, still half full
  if (leaf_page->GetSize() >= leaf_page->GetMinSize()) {
    ctx.write_set_.clear();
    root_page_id_latch_.WUnlock();
    ReleaseHeader(ctx);

    return;
  }

  // or the leaf page is root page and the size equals to zero, remove the page
  if (ctx.IsRootPage(ctx.write_set_.back().PageId())) {
    if (leaf_page->GetSize() == 0) {
      RemoveRoot(ctx, INVALID_PAGE_ID);
    }
    root_page_id_latch_.WUnlock();
    ctx.write_set_.clear();
    ReleaseHeader(ctx);

    return;
  }

  RemoveLeaf(ctx, pos);

  ctx.write_set_.clear();
  ReleaseHeader(ctx);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    // empty index tree, return end
    return INDEXITERATOR_TYPE();
  }

  ReadPageGuard guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto page = guard.As<BPlusTreePage>();
  while (!page->IsLeafPage()) {
    auto internal_page = guard.As<InternalPage>();
    guard = bpm_->FetchPageRead(internal_page->ValueAt(0));
    page = guard.template As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  Context ctx;
  root_page_id_latch_.RLock();
  // null tree
  if (root_page_id_ == INVALID_PAGE_ID) {
    root_page_id_latch_.RUnlock();
    return INDEXITERATOR_TYPE();
  }

  auto target_page_guard = GetLeaf(ctx, key);
  auto target_page = target_page_guard.template As<LeafPage>();

  ValueType val;
  int idx = 0;
  if (!target_page->Exist(key, val, &idx, comparator_)) {
    return INDEXITERATOR_TYPE();
  }

  return INDEXITERATOR_TYPE(bpm_, target_page_guard.PageId(), idx);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeaf(Context &ctx, const KeyType &key) -> ReadPageGuard {
  auto cur_guard = bpm_->FetchPageRead(root_page_id_);
  // after read the page, release the root_page_id latch
  root_page_id_latch_.RUnlock();
  auto *node = cur_guard.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto i_node = cur_guard.As<InternalPage>();

    int idx;
    auto page_id = i_node->FindChild(key, &idx, comparator_);

    assert(page_id != INVALID_PAGE_ID);

    cur_guard = bpm_->FetchPageRead(page_id);
    node = cur_guard.As<BPlusTreePage>();
  }
  return cur_guard;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafAndUpdate(Context &ctx, const KeyType &key, std::unordered_map<page_id_t, int> *pos) {
  auto cur_guard = bpm_->FetchPageWrite(root_page_id_);
  auto *node = cur_guard.AsMut<BPlusTreePage>();
  ctx.write_set_.emplace_back(std::move(cur_guard));
  ctx.root_page_id_ = root_page_id_;

  page_id_t page_id = INVALID_PAGE_ID;
  while (!node->IsLeafPage()) {
    // we have fetch the page from disk to memroy
    auto i_node = ctx.write_set_.back().AsMut<InternalPage>();

    // get the idx and the value (page_id)
    int idx;
    page_id = i_node->FindChild(key, &idx, comparator_);

    // store the hash from page_id to idx int the page
    if (pos != nullptr) {
      BUSTUB_ASSERT(idx >= 0, "invalid index");
      BUSTUB_ASSERT(page_id != INVALID_PAGE_ID, "invalid page id");

      (*pos)[page_id] = idx;
    }

    assert(page_id != INVALID_PAGE_ID);

    cur_guard = bpm_->FetchPageWrite(page_id);
    node = cur_guard.AsMut<BPlusTreePage>();
    ctx.write_set_.emplace_back(std::move(cur_guard));
  }
}

/*
  leaf:
    0-3 0， 0，1， 2， 3，idx <= 1 = 4 / 2 - 1 = maxsize / 2 - 1
    maxsize is even: up (m + 1) / 2, m - 1 = 4, up 3
      插左边 <=maxsize / 2 - 1, bound = maxsize / 2 - 1;
      右边   >= maxsize / 2,  bound = maxsize / 2

    0-2, 0, 1, 2
    m - 1 is odd: up m / 2 + 1, m -1 = 3, up 2
      左边，<=maxsize / 2, bound = maxsize / 2
      右边，             , bound = maxsize / 2 + 1
  internal:

    maxsize is even, maxsize - 1 is odd
    0 1 [2,3]
      左边 <= maxsize / 2, maxsize / 2
      右边 >= maxsize / 2 + 1, maxsize / 2 + 1
    0 1 [2,3] 4
      左边 <= maxsize / 2, maxsize / 2
      右边 > maxsize / 2 + 1
*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBound(const int &idx, const int &size, bool &insert_left) -> int {
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
void BPLUSTREE_TYPE::NewRootPage(Context &ctx, const KeyType &key, const ValueType &value) {
  // new root page, now head page has pointer points to the root page
  auto temp = bpm_->NewPageGuarded(&root_page_id_);
  auto head_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  auto root_page_guard = bpm_->FetchPageWrite(temp.PageId());
  temp.Drop();

  // new root page has been given a page id
  head_page->root_page_id_ = root_page_id_;

  auto root_page = root_page_guard.AsMut<LeafPage>();
  root_page->Init(leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  ctx.root_page_id_ = root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLeaf(Context &ctx, std::unordered_map<page_id_t, int> &pos) {
  auto cur_page_guard = std::move(ctx.write_set_.back());
  auto cur_page = cur_page_guard.AsMut<LeafPage>();
  ctx.write_set_.pop_back();

  page_id_t index_in_parent = pos[cur_page_guard.PageId()];

  auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();

  page_id_t sibling_page_id = -1;
  bool is_last = index_in_parent == parent_page->GetSize() - 1;
  if (is_last) {
    sibling_page_id = parent_page->ValueAt(index_in_parent - 1);
  } else {
    sibling_page_id = parent_page->ValueAt(index_in_parent + 1);
  }

  auto sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);
  auto right_page = sibling_page_guard.AsMut<LeafPage>();

  // if (is_last) {
  //   std::swap(cur_page, right_page);
  // }

  // merge two leaf page
  auto total_size = cur_page->GetSize() + right_page->GetSize();
  if (total_size <= cur_page->GetMaxSize()) {
    auto del_index = index_in_parent + 1;
    if (is_last) {
      del_index = index_in_parent;
      std::swap(cur_page, right_page);
    }
    right_page->MoveHalfTo(cur_page, 0, cur_page->GetSize(), right_page->GetSize());
    right_page->SetSize(0);
    cur_page->SetSize(total_size);
    cur_page->SetNextPageId(right_page->GetNextPageId());
    auto new_key = cur_page->KeyAt(0);

    RemoveInternal(ctx, pos, del_index, new_key);

    return;
  }

  root_page_id_latch_.WUnlock();

  // steal from sibling node;
  int idx_half = total_size / 2;
  if (is_last) {
    // [...|...] -> [...]
    // right -> cur
    cur_page->Move(idx_half, is_last);
    right_page->MoveHalfTo(cur_page, total_size - idx_half, 0, idx_half - cur_page->GetSize());
    cur_page->SetSize(idx_half);
    right_page->SetSize(total_size - idx_half);
    parent_page->SetKeyAt(index_in_parent, cur_page->KeyAt(0));
  } else {
    // [...] <- [...|...]
    right_page->MoveHalfTo(cur_page, 0, cur_page->GetSize(), right_page->GetSize() - idx_half);
    right_page->Move(right_page->GetSize() - idx_half, is_last);
    right_page->SetSize(idx_half);
    cur_page->SetSize(total_size - idx_half);
    parent_page->SetKeyAt(index_in_parent + 1, right_page->KeyAt(0));
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveInternal(Context &ctx, std::unordered_map<page_id_t, int> &pos, int del_index,
                                    KeyType new_key) {
  BUSTUB_ASSERT(!ctx.write_set_.empty(), "the current hold page is not possible empty");

  BUSTUB_ASSERT(del_index > 0, "the deleted page's index in parent is 0, it's possible");
  auto is_merge = true;
  while (!ctx.write_set_.empty()) {
    auto cur_page_guard = std::move(ctx.write_set_.back());
    auto cur_page = cur_page_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();

    cur_page->SetKeyAt(del_index - 1, new_key);
    if (is_merge) {
      cur_page->RemoveAt(del_index);
    }

    // current page is root page and it's size is decrease to 1
    if (ctx.IsRootPage(cur_page_guard.PageId())) {
      if (cur_page->GetSize() == 1) {
        RemoveRoot(ctx, cur_page->ValueAt(0));
      }
      root_page_id_latch_.WUnlock();
      return;
    }

    // otherwise, after delete is still half full
    if (cur_page->GetSize() >= cur_page->GetMinSize()) {
      new_key = cur_page->KeyAt(0);
      del_index = pos[cur_page_guard.PageId()] + 1;
      is_merge = false;
      root_page_id_latch_.WUnlock();
      return;
    }

    // get the parent page
    auto &parent_page_guard = ctx.write_set_.back();
    auto parent_page = parent_page_guard.AsMut<InternalPage>();

    page_id_t index_in_parent = pos[cur_page_guard.PageId()];
    page_id_t sibling_page_id = -1;

    bool is_last = index_in_parent == parent_page->GetSize() - 1;
    if (is_last) {
      sibling_page_id = parent_page->ValueAt(index_in_parent - 1);
    } else {
      sibling_page_id = parent_page->ValueAt(index_in_parent + 1);
    }

    auto sibling_page_guard = bpm_->FetchPageWrite(sibling_page_id);
    auto right_page = sibling_page_guard.AsMut<InternalPage>();

    auto total_size = cur_page->GetSize() + right_page->GetSize();
    if (total_size <= cur_page->GetMaxSize()) {
      del_index = index_in_parent + 1;
      if (is_last) {
        del_index = index_in_parent;
        std::swap(cur_page, right_page);
      }

      right_page->MoveHalfTo(cur_page, 0, cur_page->GetSize(), right_page->GetSize());
      right_page->SetSize(0);
      cur_page->SetSize(total_size);

      parent_page->SetKeyAt(del_index - 1, cur_page->KeyAt(0));
      new_key = cur_page->KeyAt(0);
    } else {  // merge
      int idx_half = total_size / 2;
      if (is_last) {
        // [...|...] -> [...]
        cur_page->Move(idx_half, is_last);
        right_page->MoveHalfTo(cur_page, total_size - idx_half, 0, idx_half - cur_page->GetSize());
        cur_page->SetSize(idx_half);
        right_page->SetSize(total_size - idx_half);

        parent_page->SetKeyAt(index_in_parent, cur_page->KeyAt(0));
        parent_page->SetKeyAt(index_in_parent - 1, right_page->KeyAt(0));

        new_key = right_page->KeyAt(0);
        del_index = index_in_parent;
      } else {
        // [...] <- [...|...]F
        right_page->MoveHalfTo(cur_page, 0, cur_page->GetSize(), right_page->GetSize() - idx_half);
        right_page->Move(right_page->GetSize() - idx_half, is_last);
        right_page->SetSize(idx_half);
        cur_page->SetSize(total_size - idx_half);

        parent_page->SetKeyAt(index_in_parent + 1, right_page->KeyAt(0));
        parent_page->SetKeyAt(index_in_parent, cur_page->KeyAt(0));

        new_key = cur_page->KeyAt(0);
        del_index = index_in_parent + 1;
      }
      is_merge = false;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveRoot(Context &ctx, const page_id_t &page_id) {
  auto header_pager = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_pager->root_page_id_ = page_id;
  ctx.root_page_id_ = page_id;
  root_page_id_ = page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FetchHeaderWrite(Context &ctx) {
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  ctx.header_page_ = std::move(header_guard);
  ctx.root_page_id_ = root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseHeader(Context &ctx) { ctx.header_page_ = std::nullopt; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
