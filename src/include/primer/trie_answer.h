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

  std::shared_ptr<TrieNode> cur(std::move(temp));
  std::shared_ptr<TrieNode> pre(nullptr);
  std::shared_ptr<TrieNode> root(cur);

  int n = key.size();
  for (int i = 0; i < n - 1; ++i) {
    if (!cur->children_.count(key[i])) {
      // for current node, clone a new node and create a new children
      // then change the children of current parrent to current new clone node
      if (i != 0) {
        cur = std::shared_ptr<TrieNode>(cur->Clone());
        pre->children_[key[i - 1]] = cur;
      }
      cur->children_[key[i]] = std::make_shared<TrieNode>();
    }

    // search down
    pre = cur;
    cur = std::const_pointer_cast<TrieNode>(cur->children_[key[i]]);
  }

  if (n != 1) {
    cur = std::shared_ptr<TrieNode>(cur->Clone());
    pre->children_[key[n - 2]] = cur;
  }

  auto vptr = std::make_shared<T>(value);

  if (!cur->children_.count(key[n - 1])) {
    // spectial for last node
    cur->children_[key[n - 1]] = std::make_shared<TrieNodeWithValue<T>>(vptr);
  } else {
    auto curchildren = cur->children_[key[n - 1]]->Clone();
    cur->children_[key[n - 1]] =
        std::make_shared<TrieNodeWithValue<T>>(curchildren->children_, vptr);
  }

  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
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

  std::shared_ptr<TrieNode> cur(root_->Clone());
  std::shared_ptr<TrieNode> pre(nullptr);
  std::shared_ptr<TrieNode> root(cur);

  int n = key.size();

  for (int i = 0; i < n - 1; ++i) {
    if (cur == nullptr or !cur->children_.count(key[i])) return *this;
    pre = cur;
    cur = std::const_pointer_cast<TrieNode>(cur->children_[key[i]]);
  }

  if (cur->children_.count(key[n - 1])) {
    // the value is not exist, just return *this
    if (!cur->children_[key[n - 1]]->is_value_node_) {
      return *this;
    }

    // new children has the same children as the old
    // just children of cur->children_[key[n - 1]]
    auto curchildren = cur->children_[key[n - 1]]->Clone();
    auto newchildren = std::make_shared<TrieNode>(curchildren->children_);

    // key has length 1
    // set the the son of parent to the cur node
    if (n != 1) {
      auto newcur = cur->Clone();
      cur = std::shared_ptr<TrieNode>(std::move(newcur));
      pre->children_[key[n - 2]] = cur;
    }

    // set the son of cur node to new children
    cur->children_[key[n - 1]] = newchildren;
    return Trie(root);
  } else {
    return *this;
  }
}

const uint32_t CASE_1_YOUR_ANSWER = 0;
const uint32_t CASE_2_YOUR_ANSWER = 0;
const uint32_t CASE_3_YOUR_ANSWER = 0;
