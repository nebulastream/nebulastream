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

#include <Runtime/BufferManager.hpp>
#include <MemoryLayout/MemoryLayout.hpp>

namespace NES::Nautilus::Interface
{

class PagedVectorRef;

class PagedVector;
using PagedVectorPtr = std::shared_ptr<PagedVector>;

/**
 * @brief Stores the pointer to the text, the length of the text and the index of the varSizedDataPages where the text begins.
 */
struct VarSizedDataEntryMapValue
{
    uint8_t* entryPtr;
    uint32_t entryLength;
    uint64_t entryBufIdx;
} __attribute__((packed));

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries. All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size. Each page can contain page_size/entry_size entries.
 * Additionally to the fixed data types, this PagedVector also supports variable sized data.
 * To know where each variable sized data lies, we store the entryPtr, the entryLength and the entryBufIdx in the
 * map at varSizedDataEntryMap[varSizedDataEntryMapCounter] and increment the varSizedDataEntryMapCounter for each new record.
 */
class PagedVector
{
public:
    PagedVector(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider, Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout);

    /**
     * @brief Appends a new page to the pages vector. It also sets the number of tuples in the TupleBuffer to capacityPerPage
     * and updates the numberOfEntriesOnCurrPage.
     */
    void appendPage();

    /**
     * @brief Appends a new page to the varSizedDataPages vector. It also updates the currVarSizedDataEntry pointer.
     */
    void appendVarSizedDataPage();

    /**
     * @brief Stores text of the given length in the varSizedDataPages. If the current page is full, a new page is appended.
     * It then creates a new entry in the varSizedDataEntryMap with the given key and the pointer to the text and its length.
     * @param text
     * @param length
     * @return uint64_t Returns the key of the new entry in the varSizedDataEntryMap.
     */
    uint64_t storeText(const int8_t* text, uint32_t length);

    /**
     * @brief Loads text from the varSizedDataPages by retrieving the pointer to the text and its length from the
     * varSizedDataEntryMap with the given key.
     * @param textEntryMapKey
     * @return int8_t*
     */
    int8_t* loadText(uint64_t textEntryMapKey);

    /// Combines the pages of the given PagedVector with the pages of this PagedVector.
    void appendAllPages(PagedVector& other);

    /// Checks if the varSizedDataEntryMap is empty.
    [[nodiscard]] bool varSizedDataEntryMapEmpty() const;

    std::vector<Memory::TupleBuffer>& getPages();
    [[nodiscard]] uint64_t getNumberOfPages() const;
    [[nodiscard]] uint64_t getNumberOfVarSizedPages() const;
    [[nodiscard]] uint64_t getNumberOfEntries() const;
    [[nodiscard]] uint64_t getNumberOfEntriesOnCurrentPage() const;
    [[nodiscard]] uint64_t getVarSizedDataEntryMapCounter() const;
    [[nodiscard]] uint64_t getEntrySize() const;
    [[nodiscard]] uint64_t getCapacityPerPage() const;
    void setLastPageRead(uint8_t* page);

private:
    friend PagedVectorRef;
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider;
    Memory::MemoryLayouts::MemoryLayoutPtr memoryLayout;
    uint64_t totalNumberOfEntries;
    uint64_t numberOfEntriesOnCurrPage;
    std::vector<Memory::TupleBuffer> pages;
    uint8_t* lastPageRead;
    std::vector<Memory::TupleBuffer> varSizedDataPages;
    uint8_t* currVarSizedDataEntry;
    std::map<uint64_t, VarSizedDataEntryMapValue> varSizedDataEntryMap;
    uint64_t varSizedDataEntryMapCounter;
};
} /// namespace NES::Nautilus::Interface
