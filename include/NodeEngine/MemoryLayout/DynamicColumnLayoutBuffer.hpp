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
    bool pushRecord(std::tuple<Types...> record);

  private:
    /**
     * Copies fields of tuple sequentially to address
     */
    template <size_t I = 0, typename... Ts>
    typename std::enable_if<I == sizeof...(Ts), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes);
    template <size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes);

    /**
     * Copies fields of tuple sequentially from address
     */
    template <size_t I = 0, typename... Ts>
    typename std::enable_if<I == sizeof...(Ts), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint64_t recordIndex, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes);
    template <size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint64_t recordIndex, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes);


  private:
    const std::vector<COL_OFFSET_SIZE> columnOffsets;
    const DynamicColumnLayout& dynamicColLayout;
    const uint8_t* basePointer;
};

template <size_t I, typename... Ts>
typename std::enable_if<I == sizeof...(Ts), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes)
{
    // Iterated through tuple, so simply return
    ((void)tup);
    ((void)fieldSizes);
    return;
}

template <size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes)
{
    // Get current type of tuple and cast address to this type pointer
    auto address = basePointer + columnOffsets[I] + fieldSizes[I] * numberOfRecords;
    *((typename std::tuple_element<I, std::tuple<Ts...>>::type *)(address)) = std::get<I>(tup);

    // Go to the next field of tuple
    copyTupleFieldsToBuffer<I + 1>(tup, fieldSizes);
}

template <size_t I, typename... Ts>
typename std::enable_if<I == sizeof...(Ts), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint64_t recordIndex, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes)
{
    // Iterated through tuple, so simply return
    ((void)tup);
    ((void)fieldSizes);
    return;
}

template <size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint64_t recordIndex, const std::vector<NES::NodeEngine::DynamicMemoryLayout::FIELD_SIZE> &fieldSizes)
{
    // Get current type of tuple and cast address to this type pointer
    auto address = basePointer + columnOffsets[I] + fieldSizes[I] * recordIndex;
    std::get<I>(tup) = *((typename std::tuple_element<I, std::tuple<Ts...>>::type *)(address));

    // Go to the next field of tuple
    copyTupleFieldsFromBuffer<I + 1>(tup, recordIndex, fieldSizes);
}

template<bool boundaryChecks, typename... Types>
bool DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    if (numberOfRecords >= capacity) {
        NES_WARNING("DynamicColumnLayoutBuffer: TupleBuffer is full and thus no tuple can be added!");
        return false;
    }

    copyTupleFieldsToBuffer(record, this->getFieldSizes());
    ++numberOfRecords;

    tupleBuffer.setNumberOfTuples(numberOfRecords);
    return true;
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    if (recordIndex >= capacity) {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutBuffer: Trying to access a record above capacity");
    }

    std::tuple<Types...> retTuple;
    copyTupleFieldsFromBuffer(retTuple, recordIndex, this->getFieldSizes());

    return retTuple;
}

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
