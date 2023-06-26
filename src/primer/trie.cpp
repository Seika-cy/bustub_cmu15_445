#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (this->root_ == nullptr) {
    return nullptr;
  }

  auto cur = this->root_;
  for (const auto &ch : key) {
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      return nullptr;
    }
    cur = it->second;
  }
  if (cur->is_value_node_) {
    auto t = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
    return t == nullptr ? nullptr : t->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr cur_old = (this->root_ != nullptr) ? this->root_ : std::make_shared<TrieNode>();
  auto value_ptr = std::make_shared<T>(std::move(value));

  std::shared_ptr new_root = cur_old->Clone();
  auto cur = new_root;

  if (key.empty()) {
    std::shared_ptr<TrieNode> t = std::make_shared<TrieNodeWithValue<T>>(new_root->children_, value_ptr);
    return Trie{t};
  }
  int n = key.size();
  for (const char &ch : key) {
    n--;
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      std::shared_ptr<TrieNode> t = std::make_shared<TrieNode>();
      if (n == 0) {
        t = std::make_shared<TrieNodeWithValue<T>>(value_ptr);
      }
      cur->children_.emplace(ch, t);
      cur = t;
    } else {
      std::shared_ptr t = it->second->Clone();
      if (n == 0) {
        t = std::make_shared<TrieNodeWithValue<T>>(t->children_, value_ptr);
      }
      cur->children_.insert_or_assign(ch, t);
      cur = t;
    }
  }
  return Trie{new_root};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::shared_ptr new_root = this->root_->Clone();
  if (this->root_ == nullptr) {
    return Trie{new_root};
  }
  if (key.empty()) {
    return Trie{std::make_shared<TrieNode>(new_root->children_)};
  }
  int n = key.size();
  auto cur = new_root;
  for (const char &ch : key) {
    n--;
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      return Trie{new_root};
    }
    std::shared_ptr t = it->second->Clone();
    if (n == 0) {
      t = std::make_shared<TrieNode>(t->children_);
    }
    cur->children_.insert_or_assign(ch, t);
    cur = t;
  }
  return Trie{new_root};
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer vaxlue) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

// template auto Trie::Put(std::string_view key, MoveBlocked) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

template <>
auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie {
  std::shared_ptr cur_old = (this->root_ != nullptr) ? this->root_ : std::make_shared<TrieNode>();
  auto value_ptr = std::make_shared<MoveBlocked>(std::move(value.wait_));

  std::shared_ptr new_root = cur_old->Clone();
  auto cur = new_root;

  if (key.empty()) {
    std::shared_ptr<TrieNode> t = std::make_shared<TrieNodeWithValue<MoveBlocked>>(new_root->children_, value_ptr);
    return Trie{t};
  }
  int n = key.size();
  for (const char &ch : key) {
    n--;
    auto it = cur->children_.find(ch);
    if (it == cur->children_.end()) {
      std::shared_ptr<TrieNode> t = std::make_shared<TrieNode>();
      if (n == 0) {
        t = std::make_shared<TrieNodeWithValue<MoveBlocked>>(value_ptr);
      }
      cur->children_.emplace(ch, t);
      cur = t;
    } else {
      std::shared_ptr t = it->second->Clone();
      if (n == 0) {
        t = std::make_shared<TrieNodeWithValue<MoveBlocked>>(t->children_, value_ptr);
      }
      cur->children_.insert_or_assign(ch, t);
      cur = t;
    }
  }
  return Trie{new_root};
}
}  // namespace bustub
