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
     * @brief TODO
     * @param bufferManager
     * @param schema
     * @param pageSize
     */
    PagedVectorVarSized(Runtime::BufferManagerPtr  bufferManager, SchemaPtr  schema, uint64_t pageSize = PAGE_SIZE);

    /**
     * @brief TODO
     */
    void appendPage();

    /**
     * @brief TODO
     */
    void appendVarSizedDataPage();

  private:
    Runtime::BufferManagerPtr bufferManager;
    SchemaPtr schema;
    uint64_t entrySize;
    uint64_t pageSize;
    std::vector<Runtime::TupleBuffer> pages;
    std::vector<Runtime::TupleBuffer> varSizedData;
    Runtime::TupleBuffer currPage;
    Runtime::TupleBuffer currVarSizedData;
};
} //NES::Nautilus::Interface

#endif//NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_PAGEDVECTOR_PAGEDVECTORVARSIZED_H
