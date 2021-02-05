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

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>
#include <stdint.h>

namespace NES::NodeEngine::DynamicMemoryLayout {


/**
 * @brief This class is dervied from DynamicLayoutBuffer. As such, it implements the abstract methods and also implements pushRecord() and readRecord() as templated methods.
 */
class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    DynamicRowLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicRowLayout& dynamicRowLayout);
    FIELD_SIZE getRecordSize() { return dynamicRowLayout.getRecordSize(); }
    const std::vector<FIELD_SIZE>& getFieldOffSets() { return dynamicRowLayout.getFieldOffSets(); }

    /**
     * @brief This function calculates the offset in the associated buffer for ithRecord and jthField in bytes
     * @param ithRecord
     * @param jthField
     * @param boundaryChecks
     * @return
     */
    uint64_t calcOffset(uint64_t recordIndex, uint64_t fieldIndex, const bool boundaryChecks) override;

    /**
     * Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer.
     * This function will memcpy every field into a predeclared tuple via std::apply(). The tuple will then be returned.
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * This function will memcpy every field from record to the associated buffer via std::apply()
     * @tparam Types
     * @param record
     */
    template<bool boundaryChecks, typename... Types>
    void pushRecord(std::tuple<Types...> record);

  private:
    const DynamicRowLayout& dynamicRowLayout;
};



template<bool boundaryChecks, typename... Types>
void DynamicRowLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    uint64_t offSet = calcOffset(numberOfRecords, 0, boundaryChecks);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicRowLayout.getFieldSizes();
    auto address = &(byteBuffer[offSet]);
    size_t fieldIndex = 0;

    // std::apply iterates over tuple and copies via memcpy the fields from retTuple to the buffer
    std::apply([&fieldIndex, &address, fieldSizes](auto&&... args) {
        ((memcpy(address, &args, fieldSizes[fieldIndex]),
          address = address + fieldSizes[fieldIndex], ++fieldIndex),
         ...);}, record);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
    NES_DEBUG("DynamicRowLayoutBuffer: numberOfRecords = " << numberOfRecords);
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    uint64_t offSet = calcOffset(recordIndex, 0, boundaryChecks);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicRowLayout.getFieldSizes();

    auto address = &(byteBuffer[offSet]);
    size_t fieldIndex = 0;

    // std::apply iterates over tuple and copies via memcpy the fields into retTuple
    std::apply([&fieldIndex, &address, fieldSizes](auto&&... args) {
        ((memcpy(&args, address, fieldSizes[fieldIndex]),
          address = address + fieldSizes[fieldIndex], ++fieldIndex),
         ...);}, retTuple);
    return retTuple;

}

}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
