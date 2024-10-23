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

#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::Nautilus::Interface
{

class PagedVector;
using PagedVectorPtr = std::shared_ptr<PagedVector>;

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size. Each page can contain page_size/entry_size entries.
 * Additionally to the fixed data types, this PagedVector also supports variable sized data.
 * To know where each variable sized data lies, we store the entryPtr, the entryLength and the entryBufIdx in the
 * map at varSizedDataEntryMap[varSizedDataEntryMapCounter] and increment the varSizedDataEntryMapCounter for each new record. TODO
 */
class PagedVector
{
public:
    PagedVector(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout);
    ~PagedVector();

    /// Appends a new page to the pages vector. It also sets the number of tuples in the TupleBuffer to capacityPerPage
    /// and updates the numberOfEntriesOnCurrPage. TODO
    Memory::TupleBuffer* appendPage();

    /// Combines the pages of the given PagedVector with the pages of this PagedVector.
    void appendAllPages(PagedVector& other);

    /// Iterates over all pages and sums up the number of tuples.
    uint64_t getTotalNumberOfEntries() const;

    std::vector<Memory::TupleBuffer>& getPages();
    [[nodiscard]] uint64_t getEntrySize() const;
    [[nodiscard]] uint64_t getCapacityPerPage() const;

private:
    const std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    const Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    std::vector<Memory::TupleBuffer> pages;
};

}
