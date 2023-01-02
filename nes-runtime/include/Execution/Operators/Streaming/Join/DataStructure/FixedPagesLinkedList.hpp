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
#ifndef NES_FIXEDPAGESLINKEDLIST_HPP
#define NES_FIXEDPAGESLINKEDLIST_HPP

#include <Execution/Operators/Streaming/Join/DataStructure/FixedPage.hpp>
#include <Runtime/Allocator/FixedPagesAllocator.hpp>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class implements a LinkedList consisting of FixedPages
 */
class FixedPagesLinkedList {
  public:
    /**
     * @brief Constructor for a FixedPagesLinkedList
     * @param fixedPagesAllocator
     * @param sizeOfRecord
     * @param pageSize
     */
    explicit FixedPagesLinkedList(FixedPagesAllocator& fixedPagesAllocator, size_t sizeOfRecord, size_t pageSize);

    /**
     * @brief Appends an item with the hash to this list by returning a pointer to a free memory space.
     * @param hash
     * @return Pointer to a free memory space where to write the data
     */
    uint8_t* append(const uint64_t hash);

    /**
     * @brief Returns all pages belonging to this list
     * @return Vector containing pointer to the FixedPages
     */
    const std::vector<std::unique_ptr<FixedPage>>& getPages() const;

  private:
    size_t pos;
    FixedPagesAllocator& fixedPagesAllocator;
    std::vector<std::unique_ptr<FixedPage>> pages;
    const size_t sizeOfRecord;
    const size_t pageSize;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_FIXEDPAGESLINKEDLIST_HPP
