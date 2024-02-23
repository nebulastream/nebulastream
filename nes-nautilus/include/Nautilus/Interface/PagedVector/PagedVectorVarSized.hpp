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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_H
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_H

#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRef;
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
     * @brief Appends a new page to the pages vector. It also sets the number of tuples in the TupleBuffer to capacityPerPage.
     */
    void appendPage();

    /**
     * @brief Appends a new page to the varSizedDataPages vector. It also updates the currVarSizedDataEntry pointer.
     */
    void appendVarSizedDataPage();

    /**
     * @brief Stores text of the given length in the varSizedDataPages. If the current page is full, a new page is appended.
     * @param text
     * @param length
     * @return uint64_t Returns the key to the text in the varSizedDataEntryMap.
     */
    uint64_t storeText(const char* text, uint32_t length);

    /**
     * @brief Loads text from the varSizedDataPages by retrieving the pointer to the text and its length from the
     * varSizedDataEntryMap with the given key.
     * @param textEntryMapKey
     * @return TextValue*
     */
    std::string loadText(uint8_t *textPtr, uint32_t length);

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

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_H
