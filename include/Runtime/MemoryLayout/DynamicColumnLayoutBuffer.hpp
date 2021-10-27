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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_BUFFER_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_BUFFER_HPP_

#include <Runtime/MemoryLayout/DynamicColumnLayout.hpp>
#include <Runtime/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <cstdint>
#include <utility>

namespace NES::Runtime::DynamicMemoryLayout {

using COL_OFFSET_SIZE = uint64_t;

/**
 * @brief This class is dervied from DynamicLayoutBuffer. As such, it implements the abstract methods and also implements pushRecord() and readRecord() as templated methods.
 * Additionally, it adds a std::vector of columnOffsets which is useful for calcOffset(). As the name suggests, it is a columnar storage and as such it serializes all fields.
 * This class is non-thread safe
 */
class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer {

  public:
    DynamicColumnLayoutBuffer(TupleBuffer tupleBuffer,
                              uint64_t capacity,
                              std::shared_ptr<DynamicColumnLayout> dynamicColLayout,
                              std::vector<COL_OFFSET_SIZE> columnOffsets);

    ~DynamicColumnLayoutBuffer() override = default;

    /**
     * @return retrieves the field sizes of the column layout
     */
    const std::vector<FIELD_SIZE>& getFieldSizes() { return dynamicColLayout->getFieldSizes(); }

    /**
     * @param fieldName
     * @return field index from the fieldName
     */
    [[nodiscard]] std::optional<uint64_t> getFieldIndexFromName(std::string fieldName) const {
        return dynamicColLayout->getFieldIndexFromName(std::move(fieldName));
    };

    /**
     * @brief This function calculates the offset in the associated buffer for ithRecord and jthField in bytes
     * @param recordIndex
     * @param fieldIndex
     * @param boundaryChecks
     * @return calculated offset
     */
    uint64_t calcOffset(uint64_t recordIndex, uint64_t fieldIndex, bool boundaryChecks) override;

    /**
     * @brief Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer.
     * @tparam Types belonging to record
     * @tparam boundaryChecks if true will check if access is allowed
     * @param record
     * @return record at given recordIndex
     */
    template<bool boundaryChecks, typename... Types>
    std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * @brief Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * @param record
     * @tparam Types belonging to record
     * @tparam boundaryChecks if true will check if access is allowed
     * @return success of this function
     */
    template<bool boundaryChecks, typename... Types>
    bool pushRecord(std::tuple<Types...> record);

    /**
     * @brief This function will write/overwrite a tuple at recordIndex position in the buffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @param record
     * @param recordIndex
     * @return success of this function
     */
    template<bool boundaryChecks, typename... Types>
    bool pushRecord(std::tuple<Types...> record, uint64_t recordIndex);

  private:
    /**
     * @brief Copies fields of tuple to buffer, by iterating over tup via template recursion
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     * @param tup tuple to be read from
     * @param fieldSizes needed for calculating correct address
     * @param recordIndex
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type
    copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                            const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes,
                            uint64_t recordIndex);
    /**
     * @brief Recursion anchor for above function
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     * @param tup tuple to be read from
     * @param fieldSizes needed for calculating correct address
     * @param recordIndex
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I == sizeof...(Ts)), void>::type
    copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                            const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes,
                            uint64_t recordIndex);

    /**
     * @brief Copies fields of buffer to tuple, by iterating over tup via template recursion
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     * @param tup tuple to be written to
     * @param recordIndex
     * @param fieldSizes needed for calculating correct address
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type
    copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                              uint64_t recordIndex,
                              const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes);

    /**
     * @brief Recursion anchor for above function
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     * @param tup tuple to be written to
     * @param recordIndex
     * @param fieldSizes needed for calculating correct address
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I == sizeof...(Ts)), void>::type
    copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                              uint64_t recordIndex,
                              const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes);

    const std::vector<COL_OFFSET_SIZE> columnOffsets;
    const std::shared_ptr<DynamicColumnLayout> dynamicColLayout;
    const uint8_t* basePointer;
};

template<size_t I, typename... Ts>
typename std::enable_if<(I == sizeof...(Ts)), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                   const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes,
                                                   uint64_t recordIndex) {
    // Finished iterating through tuple via template recursion. So all that is left is to do a simple return.
    // As we are not using any variable, we need to have them set void otherwise the compiler will throw an unused variable error.
    ((void) tup);
    ((void) fieldSizes);
    ((void) recordIndex);
}

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type
DynamicColumnLayoutBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                   const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes,
                                                   uint64_t recordIndex) {
    // Get current type of tuple and cast address to this type pointer
    const auto* address = basePointer + columnOffsets[I] + fieldSizes[I] * recordIndex;
    *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address)) = std::get<I>(tup);

    // Go to the next field of tuple
    copyTupleFieldsToBuffer<I + 1>(tup, fieldSizes, recordIndex);
}

template<size_t I, typename... Ts>
typename std::enable_if<(I == sizeof...(Ts)), void>::type DynamicColumnLayoutBuffer::copyTupleFieldsFromBuffer(
    std::tuple<Ts...>& tup,
    uint64_t recordIndex,
    const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes) {
    // Iterated through tuple, so simply return
    ((void) tup);
    ((void) fieldSizes);
    ((void) recordIndex);
}

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type DynamicColumnLayoutBuffer::copyTupleFieldsFromBuffer(
    std::tuple<Ts...>& tup,
    uint64_t recordIndex,
    const std::vector<NES::Runtime::DynamicMemoryLayout::FIELD_SIZE>& fieldSizes) {
    // Get current type of tuple and cast address to this type pointer
    const auto* address = basePointer + columnOffsets[I] + fieldSizes[I] * recordIndex;
    std::get<I>(tup) = *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address));

    // Go to the next field of tuple
    copyTupleFieldsFromBuffer<I + 1>(tup, recordIndex, fieldSizes);
}

template<bool boundaryChecks, typename... Types>
bool DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    // Calling pushRecord<>() with numberOfRecords as recordIndex.
    // This works as we are starting to count at 0 but numberOfRecords starts at 1
    // numberOfRecords will be increased by one in called function
    return pushRecord<boundaryChecks>(record, numberOfRecords);
}

template<bool boundaryChecks, typename... Types>
bool DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record, uint64_t recordIndex) {
    if (boundaryChecks && recordIndex >= capacity) {
        NES_WARNING("DynamicColumnLayoutBuffer: TupleBuffer is too small to write to position "
                    << recordIndex << " and thus no write can happen!");
        return false;
    }

    copyTupleFieldsToBuffer(record, this->getFieldSizes(), recordIndex);

    if (recordIndex + 1 > numberOfRecords) {
        numberOfRecords = recordIndex + 1;
    }

    tupleBuffer.setNumberOfTuples(numberOfRecords);
    return true;
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    if (boundaryChecks && recordIndex >= capacity) {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutBuffer: Trying to access a record above capacity");
    }

    std::tuple<Types...> retTuple;
    copyTupleFieldsFromBuffer(retTuple, recordIndex, this->getFieldSizes());

    return retTuple;
}

}// namespace NES::Runtime::DynamicMemoryLayout

#endif  // NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_BUFFER_HPP_
