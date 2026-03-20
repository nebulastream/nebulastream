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

    static PagedVector init(TupleBuffer buffer);
    static PagedVector load(const TupleBuffer& buffer);

    /// Appends a new page to the pages vector if the last page is full.
    void appendPageIfFull(AbstractBufferProvider* bufferProvider, uint64_t capacity, uint64_t bufferSize);

    /// Copies all pages from other to this
    /// @warning You have to update the cumsum from scratch after calling this method.
    void movePagesFrom(PagedVector& other);

    /// Returns the position of the buffer in the buffer provider that contains the entry at the given position.
    [[nodiscard]] std::optional<uint64_t> getBufferPosForEntry(uint64_t entryPos) const;

    [[nodiscard]] uint64_t getTotalNumberOfEntries() const;
    [[nodiscard]] uint64_t getNumberOfPages() const;
    [[nodiscard]] uint64_t getNumberOfEntriesLastPage() const;
    [[nodiscard]] bool isEmpty() const;
    const Page operator[](size_t index) const;
    [[nodiscard]] static uint64_t getBufferSize();

    /// Finds the index for an entry position
    [[nodiscard]] std::optional<size_t> findIdx(uint64_t entryPos) const;
    /// Adds an existing page into the PagedVector.
    /// @warning You have to update the cumsum from scratch after calling this method.
    void addExistingPage(TupleBuffer& newPageBuffer);
    void addNewPage(AbstractBufferProvider* bufferProvider, uint64_t bufferSize);
    void addPages(const PagedVector& other);

private:
    struct Header
    {
        uint64_t numPages;

        Header(uint64_t numPages) : numPages(numPages) { }
    };

    explicit PagedVector(TupleBuffer buffer) : buffer(std::move(buffer)) { }

    [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

    [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<Header>().data(); }

    /// We use a cumulative sum to increase the speed of findIdx for an entry pos
    void updateCumulativeSumLastItem();
    void updateCumulativeSumAllPages();

    TupleBuffer buffer;
};

}
