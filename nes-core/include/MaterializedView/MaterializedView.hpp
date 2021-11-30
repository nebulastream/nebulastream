/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_MATERIALIZEDVIEW_MATERIALIZEDVIEW_HPP_
#define NES_INCLUDE_MATERIALIZEDVIEW_MATERIALIZEDVIEW_HPP_

#include <API/Schema.hpp>

namespace NES::Experimental {

/**
 * @brief
 */
class MaterializedView {
  public:
    /**
     * @brief default constructor
     * @param bufferSize materialized view buffer size in MB
     * @param schema used schema to store data
     * @info we expect the materialized view to fit all tuples into the given buffer size
     */
    MaterializedView(uint64_t memAreaSize, Schema schema) : memAreaSize(memAreaSize), schema(schema), currentWritePosInBytes(0) {
        auto sizeMB = memAreaSize * 1024 * 1024;
        memArea = reinterpret_cast<uint8_t*>(malloc(sizeMB));
    }
    uint8_t* getMemArea() { return memArea; };
    uint64_t getMemAreaSize() { return memAreaSize; };
    uint64_t getMaxNumRecords() {
        size_t recordSize = schema.getSchemaSizeInBytes();
        return memAreaSize / recordSize;
    }
    uint64_t getCurrentWritePosInBytes() { return currentWritePosInBytes; };
    void setCurrentWritePosInBytes(uint64_t newVal) { currentWritePosInBytes = newVal; }

  private:
    uint8_t* memArea;
    uint64_t memAreaSize;
    Schema schema;
    uint64_t currentWritePosInBytes;
};
using MaterializedViewPtr = std::shared_ptr<MaterializedView>;

}// namespace NES::Experimental
#endif//NES_INCLUDE_MATERIALIZEDVIEW_MATERIALIZEDVIEW_HPP_
