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

#include <Nautilus/Interface/FixedPage/FixedPage.hpp>
#include <API/Schema.hpp>
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
 */
class PagedVector {
  public:
    static constexpr uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Creates a new paged vector with a specific entry size
     * @param allocator the allocator
     * @param entrySize the size of an entry.
     * @param pageSize the size of a page.
     */
    PagedVector(std::unique_ptr<std::pmr::memory_resource> allocator, uint64_t entrySize, uint64_t pageSize = PAGE_SIZE);

    /**
     * @brief Return the number of pages in the sequential data
     * @return uint64_t
     */
    uint64_t getNumberOfPages();

    /**
     * @brief Returns the set of pages
     * @return reference to std::vector<FixedPage>
     */
    std::vector<FixedPagePtr>& getPages();

    /**
     * @brief Clear the sequential data of pages
     */
    void clear();

    /**
     * @brief Return the total number of entries across all pages.
     * @return uint64_t
     */
    uint64_t getTotalNumberOfEntries() const;

    /**
     * @brief Sets the number of entries across all pages.
     * @param entries
     */
    void setTotalNumberOfEntries(uint64_t entries);

    /**
     * @brief Returns the capacity per page
     * @return uint64_t
     */
    uint64_t getCapacityPerPage() const;

    /**
     * @brief Returns the number of entries on the current page
     * @return uint64_t
     */
    uint64_t getNumberOfEntriesOnCurrentPage() const;

    /**
     * @brief Appends a new page and updates the current page and number of entries.
     * @return int8_t* page address
     */
    int8_t* appendPage();

    /**
     * @brief Moves the entry from the oldPos to the newPos. This will overwrite the data at the newPos
     * @param oldPos
     * @param newPos
     */
    void moveFromTo(uint64_t oldPos, uint64_t newPos) const;

    /**
     * @brief Returns the pointer to the first field of the record at pos
     * @param pos
     * @return Pointer to start of record
     */
    int8_t* getEntry(uint64_t pos) const;

    /**
     * @brief Combines this PagedVector with another one by MOVING the other.pages to these pages.
     * @param other: PagedVector that contains pages, which should be added to this one
     */
    void moveAllPages(PagedVector& other);

    /**
     * @brief Getter for the page size
     * @return uint64_t
     */
    uint64_t getPageSize() const;

    /**
     * @brief Getter for specific page in pages
     * @param pageNo
     * @return pointer to the page
     */
    int8_t* getFixedPage(const uint64_t& pageNo);

    /**
     * @brief Returns the number of the page of the record at pos
     * @param pos
     * @return number of the page
     */
    uint64_t getPageNo(uint64_t pos) const;

    /**
     * @brief Deconstructor
     */
    ~PagedVector();

  private:
    friend PagedVectorRef;
    std::unique_ptr<std::pmr::memory_resource> allocator;
    uint64_t entrySize;
    uint64_t pageSize;
    uint64_t capacityPerPage;
    std::vector<FixedPagePtr> pages;
    FixedPage* currentPage;
    uint64_t totalNumberOfEntries;
};

}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTOR_HPP_
