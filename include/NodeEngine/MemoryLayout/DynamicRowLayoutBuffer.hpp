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

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <stdint.h>

namespace NES::NodeEngine {



class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField, bool boundaryChecks) override;
    DynamicRowLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout);
    NES::NodeEngine::FIELD_SIZE getRecordSize() { return dynamicRowLayout->getRecordSize(); }
    const std::vector<FIELD_SIZE>& getFieldOffSets() { return dynamicRowLayout->getFieldOffSets(); }
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
    DynamicRowLayoutPtr dynamicRowLayout;
};

typedef std::unique_ptr<DynamicRowLayoutBuffer> DynamicRowLayoutBufferPtr;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;


template<bool boundaryChecks, typename... Types>
void DynamicRowLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    uint64_t offSet = calcOffset(numberOfRecords, 0, boundaryChecks);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicRowLayout->getFieldSizes();
    auto address = &(byteBuffer[offSet]);
    size_t I = 0;
    std::apply([&I, &address, fieldSizes](auto&&... args) {((memcpy(address, &args, fieldSizes[I]), address = address + fieldSizes[I], ++I), ...);}, record);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
    NES_DEBUG("DynamicRowLayoutBuffer: numberOfRecords = " << numberOfRecords);
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    uint64_t offSet = calcOffset(recordIndex, 0, boundaryChecks);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicRowLayout->getFieldSizes();

    auto address = &(byteBuffer[offSet]);
    size_t I = 0;
    std::apply([&I, &address, fieldSizes](auto&&... args) {((memcpy(&args, address, fieldSizes[I]), address = address + fieldSizes[I], ++I), ...);}, retTuple);
    return retTuple;

}

}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
