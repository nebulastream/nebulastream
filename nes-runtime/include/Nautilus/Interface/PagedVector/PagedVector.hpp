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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTOR_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTOR_HPP_
#include <Runtime/Allocator/MemoryResource.hpp>
#include <cstdint>
#include <memory>
#include <vector>
namespace NES::Nautilus::Interface {
class PagedVectorRef;

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries.
 * All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size.
 * Each page can contain page_size/entry_size entries.
 * TODO check if we should use FixedPage.cpp or introduce specific page class
 */
class PagedVector {
  public:
    static const uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Creates a new paged vector with a specific entry size
     * @param allocator the allocator
     * @param entrySize the size of an entry.
     * @param pageSize the size of a page.
     */
    PagedVector(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize, uint64_t pageSize = PAGE_SIZE);

    /**
     * @brief Return the number of pages in the sequential data
     * @return size_t
     */
    size_t getNumberOfPages();

    /**
     * @brief Returns the set of pages
     * @return std::vector<int8_t*>
     */
    const std::vector<int8_t*> getPages();

    /**
     * @brief Clear the sequential data of pages
     */
    void clear();

    /**
     * @brief Return the total number of entries across all pages.
     * @return size_t
     */
    size_t getNumberOfEntries();

    /**
     * @brief Sets the number of entries across all pages.
     * @param size_t
     */
    void setNumberOfEntries(size_t entries);

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

    /**
     * @brief Moves the entry from the oldPos to the newPos. This will overwrite the data at the newPos
     * @param oldPos
     * @param newPos
     */
    void moveFromTo(uint64_t oldPos, uint64_t newPos);

    /**
     * @brief Returns the pointer to the first field of the record at pos
     * @param pos
     * @return Pointer to start of record
     */
    int8_t* getEntry(uint64_t pos);

    /**
     * @brief Combines this PagedVector with another one by adding the other.pages to these pages
     * @param other: PagedVector that contains pages, which should be added to this one
     */
    void combinePagedVectors(const PagedVector& other);

    /**
     * @brief Getter for the entry size
     * @return uint64_t
     */
    uint64_t getEntrySize() const;

    /**
     * @brief Getter for the page size
     * @return uint64_t
     */
    uint64_t getPageSize() const;

    /**
     * @brief Deconstructor
     */
    ~PagedVector();

  private:
    friend PagedVectorRef;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    uint64_t entrySize;
    uint64_t pageSize;
    std::vector<int8_t*> pages;
    int8_t* currentPage;
    uint64_t numberOfEntries;
    uint64_t totalNumberOfEntries;
};

}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTOR_HPP_
