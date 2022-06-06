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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_

#include <memory>
#include <ostream>
#include <vector>

namespace NES::ExecutionEngine::Experimental::Trace {

/**
 * @brief The tag address references a function on the callstack.
 */
using TagAddress = uint64_t;

/**
 * @brief The tag identifies a specific executed operation in the interpreter.
 * It is represented by a list of all stack frame addresses between the operation and the execution root.
 */
class Tag {
  public:
    /**
     * @brief The hasher enables to leverage the tag in a std::map
     */
    class TagHasher {
      public:
        std::size_t operator()(const Tag& k) const;
    };

    /**
     * @brief Creates a new, which represents the current operation.
     * @param startAddress
     * @return Tag
     */
    static Tag createTag(TagAddress startAddress);

    /**
     * @brief Returns the return address of the root operation
     * @return TagAddress
     */
    static TagAddress createCurrentAddress();
    bool operator==(const Tag& other) const;
    friend std::ostream& operator<<(std::ostream& os, const Tag& tag);
  private:
    /**
     * @brief Constructor to create a new tag.
     * @param addresses
     */
    Tag(std::vector<TagAddress> addresses);
    std::vector<TagAddress> addresses;
    friend TagHasher;
};

}// namespace NES::ExecutionEngine::Experimental::Trace

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_TRACE_TAG_HPP_
