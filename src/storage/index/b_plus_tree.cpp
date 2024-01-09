#include <optional>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
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
  ctx_.header_page_ = std::move(guard);
  head_page->root_page_id_ = INVALID_PAGE_ID;
  root_page_id_ = INVALID_PAGE_ID;
  ctx_.header_page_ = std::nullopt;
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
  if (IsEmpty()) {
    return false;
  }

  auto targ_leaf = GetLeaf(key);
  auto *leaf_node = targ_leaf.template AsMut<LeafPage>();

  ValueType v;
  int i;
  auto existed = leaf_node->Exist(key, v, i, comparator_);

  if (!existed) {
    return false;
  }

  result->push_back(v);
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
  FetchHeaderWrite();
  
  if (IsEmpty()) {
    NewRootPage(key, value);
    ReleaseHeader();
    return true;
  }

  GetLeafAndUpdate(key);
  auto cur_guard = std::move(ctx_.write_set_.back());
  ctx_.write_set_.pop_back();
  auto *cur_page = cur_guard.template AsMut<LeafPage>();

  // 如果未满直接插入
  if (cur_page->GetSize() < cur_page->GetMaxSize()) {
    auto ok = cur_page->Insert(key, value, comparator_);
    ctx_.write_set_.clear();
    ReleaseHeader();
    return ok;
  }

  //
  ValueType temp;
  int idx;
  auto ok = cur_page->Exist(key, temp, idx, comparator_);
  if (ok) {
    ctx_.write_set_.clear();
    ReleaseHeader();
    return false;
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
  
  return InsertInternal(split_key, right_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertInternal(KeyType key, page_id_t page_id) -> bool {
  int idx;
  WritePageGuard cur_guard;
  while (!ctx_.write_set_.empty()) {
    cur_guard = std::move(ctx_.write_set_.back());
    ctx_.write_set_.pop_back();
    auto cur_page = cur_guard.AsMut<InternalPage>();

    // current page is not full, internal page, size == maxsize means full
    if (cur_page->GetSize() < cur_page->GetMaxSize()) {
      auto ok = cur_page->Insert(key, page_id, comparator_, 1);
      
      // release page and header
      ctx_.write_set_.clear();
      ReleaseHeader();
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
  
  // BUSTUB_ASSERT(ctx_.IsRootPage(leaf_guard.PageId()) || ctx_.IsRootPage(cur_guard.PageId()), "the current page is not the root page, error!");

  page_id_t new_root_page_id;
  auto _ = bpm_->NewPageGuarded(&new_root_page_id);
  auto root_guard = bpm_->FetchPageWrite(new_root_page_id);
  _.Drop();

  auto root_page = root_guard.AsMut<InternalPage>();
  root_page->Init(internal_max_size_);
  root_page->RootInit(root_page_id_, key, page_id);

  auto header_page = ctx_.header_page_->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = new_root_page_id;
  root_page_id_ = new_root_page_id;
  ctx_.root_page_id_ = new_root_page_id;
  ctx_.write_set_.clear();
  ReleaseHeader();

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *cur_page, const int &right_start, page_id_t *page_id) -> N* {
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

  cur_page->MoveHalfTo(right_page, right_start);

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
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
auto BPLUSTREE_TYPE::GetLeaf(const KeyType &key) -> BasicPageGuard {
  auto cur = bpm_->FetchPageBasic(root_page_id_);
  auto *node = cur.As<BPlusTreePage>();

  while (!node->IsLeafPage()) {
    auto i_node = cur.As<InternalPage>();
    auto child_node_page_id = i_node->FindChild(key, comparator_);

    assert(child_node_page_id != INVALID_PAGE_ID);

    cur = bpm_->FetchPageBasic(child_node_page_id);
    node = cur.As<BPlusTreePage>();
  }
  return cur;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetLeafAndUpdate(const KeyType &key) {
  auto cur_guard = bpm_->FetchPageWrite(root_page_id_);
  auto *node = cur_guard.AsMut<BPlusTreePage>();
  ctx_.write_set_.emplace_back(std::move(cur_guard));

  page_id_t page_id = INVALID_PAGE_ID;
  while (!node->IsLeafPage()) {
    // we have fetch the page from disk to memroy
    auto i_node = ctx_.write_set_.back().AsMut<InternalPage>();
    page_id = i_node->FindChild(key, comparator_);

    assert(page_id != INVALID_PAGE_ID);

    cur_guard = bpm_->FetchPageWrite(page_id);
    node = cur_guard.AsMut<BPlusTreePage>();
    ctx_.write_set_.emplace_back(std::move(cur_guard));
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
  if (idx <= (size - 1) / 2 ) {
    bound = (size - 1) / 2;
    insert_left = true;
  } else {
    bound = (size + 1) / 2;
    insert_left = false;
  }
  return bound;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::NewRootPage(const KeyType &key, const ValueType &value) {
  // new root page, now head page has pointer points to the root page
  auto root_page_guard = bpm_->NewPageGuarded(&root_page_id_);

  auto *head_page = ctx_.header_page_->AsMut<BPlusTreeHeaderPage>();
  // new root page has been given a page id
  head_page->root_page_id_ = root_page_id_;

  auto *root_page = root_page_guard.AsMut<LeafPage>();
  root_page->Init(leaf_max_size_);
  root_page->Insert(key, value, comparator_);
  ctx_.root_page_id_ = root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FetchHeaderWrite() {
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  ctx_.header_page_ = std::move(header_guard);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseHeader() {
  ctx_.header_page_ = std::nullopt;
}

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
