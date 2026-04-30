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

#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES
{

/// This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
/// Entries consume a fixed size, which has to be smaller than the page size. Each page can contain page_size/entry_size entries.
/// Additionally to the fixed data types, this PagedVector also supports variable sized data by attaching child buffers to the pages.
/// To read and write records to the PagedVector, the PagedVectorRef class should ONLY be used.
class PagedVector
{
public:
    /// @brief This class represents a single page of the paged vector.
    /// Pages store fixed-sized data directly. Varsizaed data is stored into buffers that are attached to the page as children buffers.
    /// Each page contains the cumulative sum in its header.
    struct Page
    {
        static Page init(TupleBuffer buffer);
        static Page load(TupleBuffer buffer);
        static uint64_t getHeaderSize();

        size_t getCumulativeSum();
        void setCumulativeSum(size_t newCumulativeSum);

        TupleBuffer buffer;

    private:
        explicit Page(TupleBuffer buffer) : buffer(std::move(buffer)) { }

        struct Header
        {
            uint64_t cumulativeSum;

            explicit Header(uint64_t cumulativeSum) : cumulativeSum(cumulativeSum) { }
        };

        [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

        [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }
    };

    enum PagedVectorStatus
    {
        VALID_PV = 0,
        INVALID_PV = 1,
    };

    static PagedVector init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize);
    static PagedVector load(const TupleBuffer& buffer);

    /// @brief Appends a new page to the pages vector if the last page is full.
    void appendPageIfFull(AbstractBufferProvider* bufferProvider);

    /// @brief Copies all pages from other to this
    void copyPagesFrom(AbstractBufferProvider* bufferProvider, const PagedVector& other);

    /// @brief Moves all pages from other to this. (just attaches the pages, no real "move"
    void movePagesFrom(PagedVector& other);

    [[nodiscard]] PagedVectorStatus getStatus() const;
    [[nodiscard]] uint64_t getTotalNumberOfRecords() const;
    [[nodiscard]] uint64_t getNumberOfPages() const;
    /// @brief Returns the size of the buffer for each one of the paged vector's pages.
    [[nodiscard]] uint64_t getPageBufferSize() const;
    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] uint64_t getPageCapacity() const;
    /// @brief Returns the number of records in the paged vector's last page (0 if empty)
    [[nodiscard]] uint64_t getNumberOfRecordsLastPage() const;
    [[nodiscard]] bool isEmpty() const;
    const Page operator[](size_t index) const;
    /// @brief Returns the size of the paged vector's main buffer (header) in bytes
    [[nodiscard]] static uint64_t getMainBufferSize();

    /// @brief Returns the index of a record within its page, given its global record index.
    [[nodiscard]] uint64_t getRecordIndexInPage(uint64_t recordIndex) const;
    /// @brief Returns the index of the page that stores the record at the given global record index.
    [[nodiscard]] size_t getPageIndex(uint64_t recordIndex) const;

private:
    struct Header
    {
        PagedVectorStatus status;
        uint64_t numPages;
        uint64_t pageBufferSize;
        uint64_t pageCapacity;
        uint64_t tupleSize;

        Header(uint64_t numPages, uint64_t bufferSize, uint64_t tupleSize)
            : numPages(numPages), pageBufferSize(bufferSize), tupleSize(tupleSize)
        {
            uint64_t effectiveSpace = bufferSize - Page::getHeaderSize();
            pageCapacity = effectiveSpace / tupleSize;
            PRECONDITION(pageCapacity > 0, "At least one tuple has to fit on a page!");
            status = VALID_PV;
        }
    };

    explicit PagedVector(TupleBuffer buffer) : buffer(std::move(buffer)) { }

    [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

    [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }

    /// @brief We use a cumulative sum to increase the speed of findIdx for an entry pos
    void updateCumulativeSumLastItem() const;
    void updateCumulativeSumAllPages() const;
    void addNewPage(AbstractBufferProvider* bufferProvider, uint64_t bufferSize);

    TupleBuffer buffer;
};

}
