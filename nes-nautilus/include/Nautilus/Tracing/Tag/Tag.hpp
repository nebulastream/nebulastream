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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TAG_TAG_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TAG_TAG_HPP_

#include <Nautilus/Util/Trie.hpp>
#include <memory>
#include <ostream>
#include <vector>

namespace NES::Nautilus::Tracing {

/**
 * @brief The tag address references a function on the callstack.
 */
using TagAddress = uint64_t;
using Tag = TrieNode<TagAddress>;

/**
 * @brief The tag identifies a specific executed operation in the interpreter.
 * It is represented by a list of all stack frame addresses between the operation and the execution root.
 */
class TagVector {
  public:
    /**
     * @brief Constructor to create a new tag.
     * @param addresses
     */
    TagVector(const std::vector<TagAddress>& addresses);
    friend std::ostream& operator<<(std::ostream& os, const TagVector& tag);
    const std::vector<TagAddress>& getAddresses();

  private:
    std::vector<TagAddress> addresses;
};

}// namespace NES::Nautilus::Tracing

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_TRACING_TAG_TAG_HPP_
