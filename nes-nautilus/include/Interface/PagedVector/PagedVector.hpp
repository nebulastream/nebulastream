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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct Arena;
class PagedVectorComparator;
class PagedVectorRef;

/// This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
/// Entries consume a fixed size, which has to be smaller than the page size. Each page can contain page_size/entry_size entries.
/// Additionally to the fixed data types, this PagedVector also supports variable sized data by attaching child buffers to the pages.
/// To read and write records to the PagedVector, the PagedVectorRef class should ONLY be used.
class PagedVector
{
public:
    /// @brief This class represents a single page of the paged vector.
    /// Pages store fixed-sized data directly. Varsized data is stored into buffers that are attached to the page as children buffers.
    /// Each page contains the cumulative sum in its header.
    struct Page
    {
        static void init(TupleBuffer buffer);
        static Page load(TupleBuffer buffer);
        static uint64_t getHeaderSize();

        [[nodiscard]] size_t getCumulativeSum() const;
        void setCumulativeSum(size_t newCumulativeSum);

        [[nodiscard]] uint64_t getNumberOfTuples() const;

    private:
        explicit Page(TupleBuffer buffer) : buffer(std::move(buffer)) { }

        struct Header
        {
            uint64_t cumulativeSum;

            explicit Header(uint64_t cumulativeSum) : cumulativeSum(cumulativeSum) { }
        };

        [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

        [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }

        TupleBuffer buffer;
    };

    /// Paged Vector magic numbers
    static constexpr auto VALID_PV = 8254372433832867;
    static constexpr auto INVALID_PV = 0;

    static void init(TupleBuffer buffer, uint64_t pageBufferSize, uint64_t tupleSize);
    static PagedVector load(const TupleBuffer& buffer);

    /// @brief Appends a new page to the pages vector if the last page is full.
    void appendPageIfFull(AbstractBufferProvider* bufferProvider);

    /// @brief Copies all pages from other to this
    void copyPagesFrom(AbstractBufferProvider& bufferProvider, const PagedVector& other);

    /// @brief Transfers ownership of `other`'s pages to this PagedVector by appending them as child buffers.
    /// This is a *logical* move: the underlying page TupleBuffers are not physically detached from `other.buffer`;
    /// they remain refcount-pinned by `other.buffer.children` until `other.buffer` itself is destroyed.
    /// `other` is marked INVALID_PV after the call. The caller MUST drop `other` immediately after and MUST NOT
    /// access it again — any further operation on `other` is undefined.
    void movePagesFrom(PagedVector& other);

    [[nodiscard]] uint64_t getStatus() const;
    [[nodiscard]] uint64_t getTotalNumberOfRecords() const;
    [[nodiscard]] uint64_t getNumberOfPages() const;
    /// @brief Returns the size of the buffer for each one of the paged vector's pages.
    [[nodiscard]] uint64_t getPageBufferSize() const;
    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] uint64_t getPageCapacity() const;
    [[nodiscard]] bool isEmpty() const;
    /// @brief Returns the size of the paged vector's main buffer (header) in bytes
    [[nodiscard]] static uint64_t getMainBufferSize();
    /// @brief Returns the index of the page that stores the record at the given global record index.
    [[nodiscard]] size_t getPageIndex(uint64_t recordIndex) const;

private:
    friend class PagedVectorRef;

    struct Header
    {
        uint64_t status;
        uint64_t numPages;
        uint64_t pageBufferSize;
        uint64_t pageCapacity;
        uint64_t tupleSize;

        Header(uint64_t numPages, uint64_t bufferSize, uint64_t tupleSize)
            : numPages(numPages), pageBufferSize(bufferSize), tupleSize(tupleSize)
        {
            PRECONDITION(bufferSize > Page::getHeaderSize() + tupleSize, "At least one tuple has to fit on a page!");
            const uint64_t effectiveSpace = bufferSize - Page::getHeaderSize();
            pageCapacity = effectiveSpace / tupleSize;
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
    void stableSortRecords(const PagedVectorComparator& comparator, Arena& arena);

    TupleBuffer buffer;
};

}
