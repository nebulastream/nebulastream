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
#ifndef NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
#define NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
#include <Runtime/Allocator/MemoryResource.hpp>
#include <cstdint>
#include <memory>
#include <vector>
namespace NES::Nautilus::Interface {
class StackRef;

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries.
 * All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size.
 * Each page can contain page_size/entry_size entries.
 * TODO check if we should use FixedPage.cpp or introduce specific page class
 */
class Stack {
  public:
    static const uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Creates a new stack with a specific entry size
     * @param allocator the allocator
     * @param entrySize the size of an entry.
     * TODO pass page size dynamically and adjust StackRef if needed.
     */
    Stack(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize);

    /**
     * @brief Return the number of pages in the stack
     * @return size_t
     */
    size_t getNumberOfPages();

    /**
     * @brief Returns the set of pages
     * @return std::vector<int8_t*>
     */
    const std::vector<int8_t*> getPages();

    /**
     * @brief Clear the list of pages
     */
    void clear();

    /**
     * @brief Return the total number of entries across all pages.
     * @return size_t
     */
    size_t getNumberOfEntries();

    /**
     * @brief Returns the capacity per page
     * @return size_t
     */
    size_t capacityPerPage();

    /**
     * @brief Returns the number of entries on the current page
     * @return size_t
     */
    size_t getNumberOfEntriesOnCurrentPage();

    /**
     * @brief Appends a new page and updates the current page and number of enties.
     * @return int8_t* page
     */
    int8_t* appendPage();

    ~Stack();

  private:
    friend StackRef;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    uint64_t entrySize;
    std::vector<int8_t*> pages;
    int8_t* currentPage;
    uint64_t numberOfEntries;
};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_STACK_STACK_HPP_
