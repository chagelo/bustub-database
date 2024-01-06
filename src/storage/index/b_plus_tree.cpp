#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_header_page.h"
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
  assert(root_page_id_ != INVALID_PAGE_ID);

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
  // Declaration of context instance.
  // Context ctx;
  // (void)ctx;
  // return false;
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
  // Declaration of context instance.
  Context ctx;
  if (IsEmpty()) {
    // new root page, now head page has pointer points to the root page
    auto root_page_guard = bpm_->NewPageGuarded(&root_page_id_);
    auto head_page_guard = bpm_->FetchPageBasic(header_page_id_);
    
    auto *head_page = head_page_guard.AsMut<BPlusTreeHeaderPage>();
    // new root page has been given a page id
    head_page->root_page_id_ = root_page_id_;
    
    auto *root_page = root_page_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(key, value, comparator_);

    return true;
  }

  auto leaf_page_guard = GetLeaf(key, ctx);
  auto *leaf_page = leaf_page_guard.template AsMut<LeafPage>();

  // 如果未满直接插入
  if (leaf_page->GetSize() != leaf_max_size_) {
    auto ok = leaf_page->Insert(key, value, comparator_);
    return ok;
  }
  ValueType temp;
  int idx;
  auto ok = leaf_page->Exist(key, temp, idx, comparator_);
  if (ok) {
    return false;
  }
  
  /*
    先分裂后插入，因为插入需要对内存做移动，如果在第一个块插入，则会做多余的后一个块的移动操作
    n 是 偶数，n / 2 及之前插入，[1, (n / 2 - 1)], [n / 2, n] 
    n 是 偶数，n / 2 后插入，[1, n / 2], [n / 2 + 1, n]
    n 是 奇数，n / 2 及之前插入, [1, n / 2], [n / 2 + 1, n]
    n 是 奇数，n / 2 之后插入，[1, n / 2 + 1], [n / 2 + 2, n]
  */
  // 返回第一个块的右边界以及插左边还是右边
  auto [right_bound, insert_left] = GetBound(idx, leaf_page->GetSize());
  auto right_leaf_guard = leaf_page->Split(right_bound);
  auto right_leaf_page = right_leaf_guard.template AsMut<LeafPage>();

  if (insert_left) {
    leaf_page->InsertVal(key, value, comparator_);
  } else {
    right_leaf_page->InsertVal(key, value, comparator_);
  }
  
  right_leaf_page->SetNextPageId(leaf_page->GetNextPageId()); 
  leaf_page->SetNextPageId(right_leaf_guard.PageId());

  // auto parent_page_guard = ctx.write_set_.end();
  // auto parent_page = parent_page_guard->AsMut<InternalPage>();
  // // use Context? 
  // while(true) {
  //   if (parent_page->GetSize() != internal_max_size_) {

  //   }


  //   parent_page->InsertInternal(key, right_leaf_guard.PageId(), comparator_);
  //   if (ctx.write_set_.empty()) {
  //     break;
  //   }
  //   parent_page_guard = ctx.write_set_.end();
  //   parent_page = parent_page_guard->AsMut<InternalPage>();
  // }
  // return false;
//  return InsertIntoLeaf(key, value, comparator_);

  /*
   * TODO(split)
   * 首先找到了根的位置，然后执行插入操作，
   * 1. 如果当前节点没满，那么直接插入
   * 2. 如果满了，看兄弟节点有没有没满的，然后...，如果没有兄弟结点或者兄弟结点都满了，就分裂，对父节点进行插入，如果父节点满了就在进行分裂 
   */

  // Context ctx;
  // (void)ctx;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value) -> bool {
  auto buffer_pageguard = GetLeaf(key);
  auto *page = buffer_pageguard.template AsMut<LeafPage>();

  auto before_insert_size = page->GetSize();
  auto new_size = page->Insert(key, value, comparator_);

}


INDEX_TEMPLATE_ARGUMENTS
template <typename N>
auto BPLUSTREE_TYPE::Split(N *node) -> N * {
  // 新建一个空的page
  page_id_t page_id;
  auto page = bpm_->NewPage(&page_id);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Cannot allocate new page");
  }

  N *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->SetPageType(node->GetPageType());

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(node);
    auto *new_leaf = reinterpret_cast<LeafPage *>(new_node);

    new_leaf->Init(page->GetPageId(), node->GetParentPageId(), leaf_max_size_);
    // 将原 page 的一半转移到新 page 中，（假如选择将新 page 放在原 page 右侧，则转移原 page 的右半部分）
    leaf->MoveHalfTo(new_leaf);
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    auto *new_internal = reinterpret_cast<InternalPage *>(new_node);

    new_internal->Init(page->GetPageId(), node->GetParentPageId(), internal_max_size_);
    internal->MoveHalfTo(new_internal, bpm_);
  }

  return new_node;
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
auto BPLUSTREE_TYPE::GetLeaf(const KeyType &key, Context &ctx) -> BasicPageGuard {
  auto cur = bpm_->FetchPageBasic(root_page_id_);
  auto *node = cur.AsMut<BPlusTreePage>();
  while(!node->IsLeafPage()) {
    auto *i_node = reinterpret_cast<InternalPage *>(node);
    auto child_node_page_id = i_node->FindChild(key, comparator_);

    assert(child_node_page_id != INVALID_PAGE_ID);

    cur = bpm_->FetchPageBasic(child_node_page_id);
    node = cur.AsMut<BPlusTreePage>();
  }
  return cur;
}
/*
  n 是 偶数，n / 2 及之前插入，[1, (n / 2 - 1)], [n / 2, n] 
  n 是 偶数，n / 2 后插入，[1, n / 2], [n / 2 + 1, n]
  n 是 奇数，n / 2 及之前插入, [1, n / 2], [n / 2 + 1, n]
  n 是 奇数，n / 2 之后插入，[1, n / 2 + 1], [n / 2 + 2, n]
*/

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetBound(const int &idx, const int &size) -> std::pair<int, bool>{
  int bound = idx;
  bool left = true;
  if (size % 2 == 0) {
    if (idx <=size / 2 - 2) {
      bound = size / 2 - 2;
    } else {
      bound = size / 2 - 1;
      left = false;
    }
  } else {
    if (idx <= size / 2 - 1) {
      bound = size / 2 - 1;
    } else {
      bound = size / 2;
      left = false;
    }
  }
  return {bound, left};
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
