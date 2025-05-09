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
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::Nautilus::Interface
{

/// This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
/// Entries consume a fixed size, which has to be smaller than the page size. Each page can contain page_size/entry_size entries.
/// Additionally to the fixed data types, this PagedVector also supports variable sized data by attaching child buffers to the pages.
/// To read and write records to the PagedVector, the PagedVectorRef class should ONLY be used.
class PagedVector
{
public:
    struct TupleBufferWithCumulativeSum
    {
        explicit TupleBufferWithCumulativeSum(const Memory::TupleBuffer& buffer) : cumulativeSum(0), buffer(buffer) { }
        size_t cumulativeSum;
        Memory::TupleBuffer buffer;
    };

    PagedVector() = default;

    /// Appends a new page to the pages vector if the last page is full.
    void appendPageIfFull(Memory::AbstractBufferProvider* bufferProvider, const Memory::MemoryLayouts::MemoryLayout* memoryLayout);

    /// Appends the pages of the given PagedVector with the pages of this PagedVector.
    void appendAllPages(PagedVector& other);

    /// Copies all pages from other to this
    void copyFrom(const PagedVector& other);

    /// Returns a pointer to the tuple buffer that contains the entry at the given position.
    [[nodiscard]] const Memory::TupleBuffer* getTupleBufferForEntry(uint64_t entryPos) const;
    /// Returns the position of the buffer in the buffer provider that contains the entry at the given position.
    [[nodiscard]] std::optional<uint64_t> getBufferPosForEntry(uint64_t entryPos) const;

    /// Iterates over all pages and sums up the number of tuples.
    [[nodiscard]] uint64_t getTotalNumberOfEntries() const;
    [[nodiscard]] const Memory::TupleBuffer& getLastPage() const;
    [[nodiscard]] const Memory::TupleBuffer& getFirstPage() const;
    [[nodiscard]] uint64_t getNumberOfPages() const;

private:
    void updateCumulativeSumLastItem();
    void updateCumulativeSumAllPages();
    std::optional<size_t> findIdx(const uint64_t entryPos) const;

    std::vector<TupleBufferWithCumulativeSum> pages;
};

}
