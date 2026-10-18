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

/// This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
/// Entries consume a fixed size, which has to be smaller than the buffer size. Each page, a buffer of the default size, can contain buffer_size/entry_size entries.
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
        static Page load(const TupleBuffer& buffer);
        static uint64_t getHeaderSize();

        [[nodiscard]] size_t getCumulativeSum() const;
        void setCumulativeSum(size_t newCumulativeSum);

        [[nodiscard]] uint64_t getNumberOfTuples() const;

    private:
        /// ponytail: a view, not an owner. The caller's buffer has to outlive the Page; this saves an atomic retain/release per access.
        explicit Page(const TupleBuffer& buffer) : buffer(const_cast<TupleBuffer&>(buffer)) { }

        struct Header
        {
            uint64_t cumulativeSum;

            explicit Header(uint64_t cumulativeSum) : cumulativeSum(cumulativeSum) { }
        };

        [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

        [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }

        TupleBuffer& buffer;
    };

    /// Paged Vector magic numbers
    static constexpr auto VALID_PV = 8254372433832867;
    static constexpr auto INVALID_PV = 0;

    /// @param inlineSizeInBytes the space in the main buffer behind the header that stores the first records, instead of a page. The main
    /// buffer then acts like a page in front of the first one, which saves a buffer for vectors with few records. It has to be at least
    /// getMainBufferSize(inlineSizeInBytes) large, otherwise the inline space is what fits. The records of a vector with inline space
    /// cannot be moved or copied to another vector. Child buffers of the inline records (var sized data) are children of the main buffer,
    /// in front of the pages.
    /// The pages are buffers of the provider's default size, which is what the provider has to hand out later on.
    static void init(TupleBuffer buffer, const AbstractBufferProvider& bufferProvider, uint64_t tupleSize, uint64_t inlineSizeInBytes = 0);
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
    /// @brief Counts one record that was written to the last page. Writers have to call this for every record they add.
    void incrementNumberOfRecords();
    [[nodiscard]] uint64_t getNumberOfPages() const;
    /// @brief Returns the size of the buffer for each one of the paged vector's pages.
    [[nodiscard]] uint64_t getPageBufferSize() const;
    [[nodiscard]] uint64_t getTupleSize() const;
    [[nodiscard]] uint64_t getPageCapacity() const;
    /// @brief The number of records that fit into the main buffer behind the header, 0 if the vector has no inline page.
    [[nodiscard]] uint64_t getInlineCapacity() const;
    /// @brief The child index of a page in the main buffer
    [[nodiscard]] ChildBufferIndex getPageChildIndex(uint64_t pageIndex) const;
    /// @brief The address of the record at the position in the vector, which is stored in the main buffer or in one of the pages.
    [[nodiscard]] int8_t* locateRecord(uint64_t recordIndex) const;
    /// @brief Makes room for one more record, counts it and returns its address. Appends a page if the last one is full.
    /// The caller writes the record to the address. This is the runtime counterpart of PagedVectorRef::pushBack for records without var sized data.
    [[nodiscard]] int8_t* reserveRecordSlot(AbstractBufferProvider* bufferProvider);
    /// @brief The address of the record at `indexInPage` in `page`, which is either one of the pages or the main buffer.
    [[nodiscard]] int8_t* getRecordAddress(const TupleBuffer& page, uint64_t indexInPage) const;
    [[nodiscard]] bool isEmpty() const;
    /// @brief Returns the size of the paged vector's main buffer (header) in bytes
    /// @brief Returns the size of the paged vector's main buffer (header and inline space) in bytes
    [[nodiscard]] static uint64_t getMainBufferSize(uint64_t inlineSizeInBytes = 0);
    /// @brief Returns the index of the page that stores the record at the given global record index.
    [[nodiscard]] size_t getPageIndex(uint64_t recordIndex) const;

private:
    struct Header
    {
        uint64_t status;
        uint64_t numPages;
        /// Number of records over all pages, which saves loading pages to count them.
        uint64_t numRecords = 0;
        uint64_t pageBufferSize;
        uint64_t pageCapacity;
        uint64_t tupleSize;
        /// Number of records that are stored behind the header of the main buffer. Pages only hold the records after these.
        uint64_t inlineCapacity = 0;
        /// The child index of the first page in the main buffer, as the children in front of it belong to the inline records.
        uint64_t firstPageChildIndex = 0;

        Header(uint64_t numPages, uint64_t bufferSize, uint64_t tupleSize)
            : numPages(numPages), pageBufferSize(bufferSize), tupleSize(tupleSize)
        {
            PRECONDITION(bufferSize > Page::getHeaderSize() + tupleSize, "At least one tuple has to fit on a page!");
            const uint64_t effectiveSpace = bufferSize - Page::getHeaderSize();
            pageCapacity = effectiveSpace / tupleSize;
            status = VALID_PV;
        }
    };

    /// A view, not an owner: the caller's buffer has to outlive the PagedVector.
    explicit PagedVector(const TupleBuffer& buffer) : buffer(const_cast<TupleBuffer&>(buffer)) { }

    [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

    [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }

    /// @brief We use a cumulative sum to increase the speed of findIdx for an entry pos
    void updateCumulativeSumLastItem() const;
    void updateCumulativeSumAllPages() const;
    void addNewPage(AbstractBufferProvider* bufferProvider);

    TupleBuffer& buffer;
};

}
