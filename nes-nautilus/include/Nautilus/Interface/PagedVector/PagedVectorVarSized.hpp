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

namespace NES::Nautilus::Interface {
class PagedVectorVarSizedRef;
class PagedVectorVarSized {
  public:
    static constexpr uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Constructor
     * @param bufferManager
     * @param schema
     * @param pageSize
     */
    PagedVectorVarSized(Runtime::BufferManagerPtr bufferManager, SchemaPtr schema, uint64_t pageSize = PAGE_SIZE);

    /**
     * @brief Appends a new page to the pages vector
     */
    void appendPage();

    /**
     * @brief Appends a new page to the varSizedDataPages vector
     */
    void appendVarSizedDataPage();

    /**
     * @brief Stores text of the given length in the varSizedDataPages. If the current page is full, a new page is appended.
     * @param text
     * @param length
     */
    void storeText(const char* text, uint64_t length);

    /**
     * @brief Loads text from the varSizedDataPages by retrieving the pointer to the text from the page at the given entryPos.
     * @param textPtr
     * @param length
     * @return std::string
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
    // TODO currently the varSizedData must fit into one single page
    std::vector<Runtime::TupleBuffer> varSizedDataPages;
    uint8_t* currVarSizedDataEntry;
};
} //NES::Nautilus::Interface

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_H
