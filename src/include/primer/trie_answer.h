#include <iostream>
#include <memory>
#include <typeinfo>

#include "trie.h"
// TODO(student): fill your answer here

using namespace bustub;

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // key is empyt, return value of root
  if (key.empty()) {
    if (!root_) {
      return nullptr;
    } else {
      if (!root_->is_value_node_) {
        return nullptr;
      } else {
        auto temp =
            std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root_);
        return temp->value_.get();
      }
    }
  }

  std::shared_ptr<const TrieNode> cur = root_;

  for (auto &c : key) {
    if (cur == nullptr or !cur->children_.count(c)) return nullptr;
    cur = cur->children_.at(c);
  }

  if (!cur->is_value_node_) {
    return nullptr;
  } else {

    // for dynamic_pointer_cast
    auto newcur = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
    if (newcur) {
      return newcur->value_.get();
    } else {
      return nullptr;
    }
  }
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  if (key.empty()) {
    auto vptr = std::make_shared<T>(value);
    // TrieNodeWithValue constructor receive shared_ptr<T>
    if (!root_) {
      auto newroot = std::make_shared<TrieNodeWithValue<T>>(vptr);
      return Trie(newroot);
    } else {
      auto temp = root_->Clone();
      auto newroot =
          std::make_shared<TrieNodeWithValue<T>>(temp->children_, vptr);
      return Trie(newroot);
    }
  }

  std::shared_ptr<TrieNode> temp(nullptr);

  if (!root_) {
    temp = std::make_shared<TrieNode>();
  } else {
    temp = root_->Clone();
  }

  auto oldcur = std::const_pointer_cast<TrieNode>(root_);
  std::shared_ptr<TrieNode> cur(std::move(temp));
  std::shared_ptr<TrieNode> pre(nullptr);
  std::shared_ptr<TrieNode> root(cur);

  int n = key.size();
  for (int i = 0; i < n; ++i) {
    if (i != 0) {
      if (oldcur) {
        // otherwise, copy the oldversion node (and its children)        
        cur = std::shared_ptr<TrieNode>(oldcur->Clone());
        
        // down to search the key[i];
        if (oldcur->children_.count(key[i])) {
          oldcur = std::const_pointer_cast<TrieNode>(oldcur->children_[key[i]]);
        } else {
          oldcur = nullptr;
        }
      } else {
        // the correspond old verision node is nullptr, just create a new node
        cur = std::make_shared<TrieNode>();

      }

      pre->children_[key[i - 1]] = cur;
      pre = cur;

    } else {
      if (oldcur) {
        if (oldcur->children_.count(key[i])) {
          oldcur = std::const_pointer_cast<TrieNode>(oldcur->children_[key[i]]);
        } else {
          oldcur = nullptr;
        }
      }
      pre = cur;
    }
  }


  // first copy children, then call TrieNodeWithValue(children_, value)
  if (oldcur) {
    // copy children
    cur = std::shared_ptr<TrieNode>(oldcur->Clone());

  } else {
    // else create a new node with no children
    cur = std::make_shared<TrieNode>();

  }
  auto vptr = std::make_shared<T>(value);
  pre->children_[key[n - 1]] = std::make_shared<TrieNodeWithValue<T>>(cur->children_, vptr);

  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  /* 
  if cannot find the key, return this
    1. root is nullptr
    2. cannot the node with the given key
    3. the found node is not a value node
  */

  // for delete, one way is to set is_value_node_ to false;
  if (root_ == nullptr) return *this;

  if (key.empty()) {
    if (!root_->is_value_node_) {
      return *this;
    } else {
      auto temp = std::make_shared<TrieNode>(root_->Clone()->children_);
      return Trie(temp);
    }
  }

  auto oldcur = std::const_pointer_cast<TrieNode>(root_);
  std::shared_ptr<TrieNode> cur(root_->Clone());
  std::shared_ptr<TrieNode> pre(nullptr);
  std::shared_ptr<TrieNode> root(cur);

  int n = key.size();

  for (int i = 0; i < n; ++i) {
    // not find the key to be removed, return current trie
    if (!oldcur or !oldcur->children_.count(key[i])) return *this;

    // for root, we have create a node for root
    if (i != 0) {
      cur = std::shared_ptr<TrieNode>(oldcur->Clone());
      pre->children_[key[i - 1]] = cur;
    }
    pre = cur;
    oldcur = std::const_pointer_cast<TrieNode>(oldcur->children_[key[i]]);
  }

  if (!oldcur->is_value_node_) return *this;

  // first copy children, then set parent
  cur = std::make_shared<TrieNode>(oldcur->Clone()->children_);
  pre->children_[key[n - 1]] = cur;

  return Trie(root);
}

const uint32_t CASE_1_YOUR_ANSWER = 0;
const uint32_t CASE_2_YOUR_ANSWER = 0;
const uint32_t CASE_3_YOUR_ANSWER = 0;
