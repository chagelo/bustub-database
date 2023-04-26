#include <string_view>

#include "common/exception.h"
#include "primer/trie.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key.
  // If the node doesn't exist, return nullptr. After you find the node, you
  // should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is
  // mismatched, and you should return nullptr. Otherwise, return the value.

  // key is empyt, return value of root
  if (key.empty()) {
    if (!root_ || !root_->is_value_node_) {
      return nullptr;
    }
    auto temp = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(root_);
    return temp->value_.get();
  }

  std::shared_ptr<const TrieNode> cur = root_;

  for (auto &c : key) {
    if (cur == nullptr || !cur->children_.count(c)) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }

  if (!cur->is_value_node_) {
    return nullptr;
  }

  // for dynamic_pointer_cast
  auto newcur = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur);
  if (newcur) {
    return newcur->value_.get();
  }
  // newcur is nullptr, means type not match
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when
  // creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the
  // node corresponding to the key already exists, you should create a new
  // `TrieNodeWithValue`.

  if (key.empty()) {
    auto vptr = std::make_shared<T>(std::move(value));
    // TrieNodeWithValue constructor receive shared_ptr<T>
    if (!root_) {
      auto newroot = std::make_shared<TrieNodeWithValue<T>>(vptr);
      return Trie(newroot);
    }
    // root_ is not nullptr, and insert value to root_
    auto temp = root_->Clone();
    auto newroot = std::make_shared<TrieNodeWithValue<T>>(temp->children_, vptr);
    return Trie(newroot);
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
  auto vptr = std::make_shared<T>(std::move(value));
  pre->children_[key[n - 1]] = std::make_shared<TrieNodeWithValue<T>>(cur->children_, vptr);

  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node
  // doesn't contain a value any more, you should convert it to `TrieNode`. If a
  // node doesn't have children any more, you should remove it.

  /*
  if cannot find the key, return this
    1. root is nullptr
    2. cannot the node with the given key
    3. the found node is not a value node
  */

  // for delete, one way is to set is_value_node_ to false;
  if (root_ == nullptr) {
    return *this;
  }

  if (key.empty()) {
    // root is not value node
    if (!root_->is_value_node_) {
      return *this;
    }
    // root is value node
    auto temp = std::make_shared<TrieNode>(root_->Clone()->children_);
    return Trie(temp);
  }
  auto oldcur = std::const_pointer_cast<TrieNode>(root_);
  std::shared_ptr<TrieNode> cur(root_->Clone());
  std::shared_ptr<TrieNode> pre(nullptr);
  std::shared_ptr<TrieNode> root(cur);

  int n = key.size();

  for (int i = 0; i < n; ++i) {
    // not find the key to be removed, return current trie
    if (!oldcur || oldcur->children_.count(key[i]) == 0) {
      return *this;
    }

    // for root, we have create a node for root
    if (i != 0) {
      cur = std::shared_ptr<TrieNode>(oldcur->Clone());
      pre->children_[key[i - 1]] = cur;
    }

    pre = cur;
    oldcur = std::const_pointer_cast<TrieNode>(oldcur->children_[key[i]]);
  }

  if (!oldcur->is_value_node_) {
    return *this;
  }

  // first copy children, then set parent
  cur = std::make_shared<TrieNode>(oldcur->Clone()->children_);
  pre->children_[key[n - 1]] = cur;

  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and
// functions in the header file. However, we separate the implementation into a
// .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate
// them here, so that they can be picked up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below
// lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
