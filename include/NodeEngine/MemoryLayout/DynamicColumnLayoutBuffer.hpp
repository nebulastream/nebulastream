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

#ifndef NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
#define NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP

#include "DynamicColumnLayout.hpp"
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <stdint.h>

namespace NES::NodeEngine {

class DynamicColumnLayoutBuffer;
typedef std::unique_ptr<DynamicColumnLayoutBuffer> DynamicColumnLayoutBufferPtr;

typedef uint64_t COL_OFFSET_SIZE;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;

class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer{

  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField, bool boundaryChecks) override;
    DynamicColumnLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicColumnLayoutPtr dynamicColLayout, std::vector<COL_OFFSET_SIZE> columnOffsets);
    const std::vector<FIELD_SIZE>& getFieldSizes() { return dynamicColLayout->getFieldSizes(); }
    /**
     * Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    void pushRecord(std::tuple<Types...> record);


  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    DynamicColumnLayoutPtr dynamicColLayout;
};



template<bool boundaryChecks, typename... Types>
void DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicColLayout->getFieldSizes();

    auto address = &(byteBuffer[0]);
    size_t I = 0;
    auto fieldAddress = address + calcOffset(numberOfRecords, I, boundaryChecks);
    std::apply([&I, &address, &fieldAddress, fieldSizes, this](auto&&... args) {((fieldAddress = address + calcOffset(numberOfRecords, I, boundaryChecks), memcpy(fieldAddress, &args, fieldSizes[I]), ++I), ...);}, record);
    tupleBuffer.setNumberOfTuples(++numberOfRecords);
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicColLayout->getFieldSizes();

    auto address = &(byteBuffer[0]);
    size_t I = 0;
    unsigned char* fieldAddress;
    std::apply([&I, address, &fieldAddress, recordIndex, fieldSizes, this](auto&&... args) {((fieldAddress = address + calcOffset(recordIndex, I, boundaryChecks), memcpy(&args, fieldAddress, fieldSizes[I]), ++I), ...);}, retTuple);


    return retTuple;
}

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
