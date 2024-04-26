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
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRef;

struct VarSizedDataEntryMapValue {
    uint8_t* entryPtr;
    uint32_t entryLength;
};

/**
 * @brief This class provides a dynamically growing stack/list data structure of entries.
 * All data is stored in a list of pages.
 * Entries consume a fixed size, which has to be smaller then the page size.
 * Each page can contain page_size/entry_size entries.
 * Additionally to the fixed data types, this PagedVector also supports variable sized data. Currently,
 * the same constraints apply to the variable sized data part as to the fixed data part.
 * To know where each variable sized data lies, we store the entryPtr and the entryLength in the
 * map varSizedDataEntryMap[varSizedDataEntryMapCounter] and increment for each new record varSizedDataEntryMapCounter.
 *
 * There are already issues solving these shortcoming that will improve this PagedVector implementation.
 * - #4638: Add support for variable sized data larger than the specified PAGE_SIZE
 * - #4639: Optimize appendAllPages()
 * - #4658: Add MemoryLayout class for PagedVectorVarSized to support different layouts
 */
class PagedVectorVarSized {
  public:
    static constexpr uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Constructor. It calculates the entrySize and the capacityPerPage based on the schema and the pageSize.
     * @param bufferManager
     * @param schema
     * @param pageSize
     */
    PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager, SchemaPtr schema, uint64_t pageSize = PAGE_SIZE);

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

  private:
    friend PagedVectorVarSizedRef;
    Runtime::BufferManagerPtr bufferManager;
    SchemaPtr schema;
    uint64_t entrySize;
    uint64_t pageSize;
    uint64_t capacityPerPage;
    uint64_t totalNumberOfEntries;
    uint64_t numberOfEntriesOnCurrPage;
    std::vector<Runtime::TupleBuffer> pages;
    std::vector<Runtime::TupleBuffer> varSizedDataPages;
    uint8_t* currVarSizedDataEntry;
    std::map<uint64_t, VarSizedDataEntryMapValue> varSizedDataEntryMap;
    uint64_t varSizedDataEntryMapCounter;
};
} //NES::Nautilus::Interface

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_HPP_
