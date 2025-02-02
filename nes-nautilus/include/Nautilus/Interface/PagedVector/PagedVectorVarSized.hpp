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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_HPP_

#include <API/Schema.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <span>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRef;

class PagedVectorVarSized;
using PagedVectorVarSizedPtr = std::shared_ptr<PagedVectorVarSized>;

struct VarSizedDataEntryMapValue {
    uint8_t* entryPtr;
    uint32_t entryLength;
    uint64_t entryBufIdx;
};

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries.
 * All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size.
 * Each page can contain page_size/entry_size entries.
 * Additionally to the fixed data types, this PagedVector also supports variable sized data. Currently,
 * similar constraints apply to the variable sized data part as to the fixed data part.
 * To know where each variable sized data lies, we store the entryPtr, the entryLength and the entryBufIdx in the
 * map varSizedDataEntryMap[varSizedDataEntryMapCounter] and increment for each new record varSizedDataEntryMapCounter.
 *
 * There are already issues solving these shortcoming that will improve this PagedVector implementation.
 * - #4639: Optimize appendAllPages()
 * - #4658: Add MemoryLayout class for PagedVectorVarSized to support different layouts
 */
class PagedVectorVarSized {
  public:
    static constexpr uint64_t PAGE_SIZE = 1024;

    /**
     * @brief Constructor. It calculates the entrySize and the capacityPerPage based on the schema and the pageSize.
     * @param bufferManager - manager to get new buffers
     * @param schema - schema of tuples associated with paged vector
     * @param pageSize - size of page to get from buffer manager
     */
    PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager, SchemaPtr schema, uint64_t pageSize = PAGE_SIZE);

    /**
     * @brief Constructor. Recreated PagedVectorVarSized from tuple buffers [Recreation not implemented for variable sized pages right now]
     * @param bufferManager - manager to get new buffers
     * @param schema - schema of tuples associated with paged vector
     * @param buffers - buffers to recreate paged vector from
     * @param pageSize - size of page to get from buffer manager
     */
    PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager,
                        SchemaPtr schema,
                        std::span<const Runtime::TupleBuffer> buffers,
                        uint64_t pageSize = PAGE_SIZE);

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
    uint64_t storeText(const char* text, uint32_t length);

    /**
     * @brief Loads text from the varSizedDataPages by retrieving the pointer to the text and its length from the
     * varSizedDataEntryMap with the given key.
     * @param textEntryMapKey
     * @return TextValue*
     */
    TextValue* loadText(uint64_t textEntryMapKey);

    /**
     * @brief Combines the pages of the given PagedVectorVarSized with the pages of this PagedVectorVarSized.
     * @param other PagedVectorVarSized
     */
    void appendAllPages(PagedVectorVarSized& other);

    /**
     * Removes the specified amount of pages from the vector of tupleBuffers that stores the fixed sized data of records.
     * The pages should only be removed if they were previously filled with records, that were now deleted, so new records can be
     * written to the last page that still contains records.
     * @param numberOfEmptyTrailingPages
     */
    void removeEmptyTrailingPages(uint64_t numberOfEmptyTrailingPages);

    /**
     * Removes the specified amount of pages from the vector of tupleBuffers that stores the var sized data for text of records.
     * The pages should only be removed if they were previously filled with text, that is now deleted, so new text can be
     * written to the last page that still contains text.
     * @param firstInvalidVarSizedPtr the pointer to the first written var sized Entry that is now invalid. (delete everything after)
     */
    void removeEmptyTrailingVarSizedPages(uint64_t firstInvalidVarSizedPtr);

    /**
     * @brief Getter for the pages object.
     * @return std::vector<Runtime::TupleBuffer>&
     */
    std::vector<Runtime::TupleBuffer>& getPages();

    /**
     * @brief Returns the number of pages.
     * @return uint64_t
     */
    uint64_t getNumberOfPages();

    /**
     * @brief Returns the number of varSizedPages.
     * @return uint64_t
     */
    uint64_t getNumberOfVarSizedPages();

    /**
     * @brief Return the total number of entries across all pages.
     * @return uint64_t
     */
    uint64_t getNumberOfEntries() const;

    /**
     * @brief Returns the number of entries on the current page.
     * @return uint64_t
     */
    uint64_t getNumberOfEntriesOnCurrentPage() const;

    /**
     * @brief Checks if the varSizedDataEntryMap is empty.
     * @return bool
     */
    bool varSizedDataEntryMapEmpty() const;

    /**
     * @brief Returns the counter of the varSizedDataEntryMap.
     * @return uint64_t
     */
    uint64_t getVarSizedDataEntryMapCounter() const;

    /**
     * @brief Returns the entry size.
     * @return uint64_t
     */
    uint64_t getEntrySize() const;

    /**
     * @brief Returns the page capacity.
     * @return uint64_t
     */
    uint64_t getCapacityPerPage() const;

  private:
    /**
     * @brief Calculates the size of one entry in page according to schema, assigns to entrySize and also calculates capacity per page.
     */
    void setEntrySizeAndCapacityPerPage();

    friend PagedVectorVarSizedRef;
    Runtime::BufferManagerPtr bufferManager;
    SchemaPtr schema;
    uint64_t pageSize;
    uint64_t entrySize;
    uint64_t capacityPerPage;
    uint64_t totalNumberOfEntries;
    uint64_t numberOfEntriesOnCurrPage;
    std::vector<Runtime::TupleBuffer> pages;
    std::vector<Runtime::TupleBuffer> varSizedDataPages;
    uint8_t* currVarSizedDataEntry;
    std::map<uint64_t, VarSizedDataEntryMapValue> varSizedDataEntryMap;
    uint64_t varSizedDataEntryMapCounter;
};
}// namespace NES::Nautilus::Interface

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_HPP_
