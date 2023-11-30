/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_TRIE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_TRIE_HPP_
#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

namespace NES::Nautilus::Tracing {

/**
 * @brief A basic trie data structure, which stores sequences of values.
 * @tparam T
 */
template<typename T>
class TrieNode {
  public:
    explicit TrieNode() : content(0) {}
    explicit TrieNode(T value) : content(value) {}

    /**
     * @brief Appends a value to the trie and returns a ptr to the element in the node in the trie.
     * @param value
     * @return TrieNode<T>*
     */
    TrieNode<T>* append(T& value) {
        auto found = std::find_if(children.begin(), children.end(), [value](std::unique_ptr<TrieNode<T>>& x) {
            return x->content == value;
        });
        if (found == children.end()) {
            return children.emplace_back(std::make_unique<TrieNode<T>>(value)).get();
        } else {
            return found->get();
        }
    };

  private:
    T content;
    std::vector<std::unique_ptr<TrieNode<T>>> children;
};
}// namespace NES::Nautilus::Tracing

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_UTIL_TRIE_HPP_
