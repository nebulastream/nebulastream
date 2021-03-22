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

#include <NodeEngine/MemoryLayout/DynamicColumnLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <stdint.h>

namespace NES::NodeEngine::DynamicMemoryLayout {

typedef uint64_t COL_OFFSET_SIZE;

/**
 * @brief This class is dervied from DynamicLayoutBuffer. As such, it implements the abstract methods and also implements pushRecord() and readRecord() as templated methods.
 * Additionally, it adds a std::vector of columnOffsets which is useful for calcOffset(). As the name suggests, it is a columnar storage and as such it serializes all fields.
 */
class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer {

  public:
    DynamicColumnLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicColumnLayout& dynamicColLayout,
                              std::vector<COL_OFFSET_SIZE> columnOffsets);
    const std::vector<FIELD_SIZE>& getFieldSizes() { return dynamicColLayout.getFieldSizes(); }
    std::optional<uint64_t> getFieldIndexFromName(std::string fieldName) const { return dynamicColLayout.getFieldIndexFromName(fieldName); };

    /**
     * @brief This function calculates the offset in the associated buffer for ithRecord and jthField in bytes
     * @param recordIndex
     * @param fieldIndex
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
    bool pushRecord(std::tuple<Types...> record);

  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    const DynamicColumnLayout& dynamicColLayout;
};

template<bool boundaryChecks, typename... Types>
bool DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    if (numberOfRecords >= capacity) {
        NES_WARNING("TupleBuffer is full and thus no tuple can be added!");
        return false;
    }
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicColLayout.getFieldSizes();

    auto address = &(byteBuffer[0]);
    size_t tupleIndex = 0;
    auto fieldAddress = address + calcOffset(numberOfRecords, tupleIndex, boundaryChecks);

    // std::apply iterates over tuple and copies via memcpy the fields from retTuple to the buffer
    std::apply(
        [&tupleIndex, &address, &fieldAddress, fieldSizes, this](auto&&... args) {
            ((fieldAddress = address + calcOffset(numberOfRecords, tupleIndex, boundaryChecks),
              memcpy(fieldAddress, &args, fieldSizes[tupleIndex]), ++tupleIndex),
             ...);
        },
        record);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
    return true;
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicColLayout.getFieldSizes();

    auto address = &(byteBuffer[0]);
    size_t tupleIndex = 0;
    unsigned char* fieldAddress;

    // std::apply iterates over tuple and copies via memcpy the fields into retTuple
    std::apply(
        [&tupleIndex, address, &fieldAddress, recordIndex, fieldSizes, this](auto&&... args) {
            ((fieldAddress = address + calcOffset(recordIndex, tupleIndex, boundaryChecks),
              memcpy(&args, fieldAddress, fieldSizes[tupleIndex]), ++tupleIndex),
             ...);
        },
        retTuple);

    return retTuple;
}

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
