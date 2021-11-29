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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_BUFFER_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_BUFFER_HPP_

#include <Runtime/MemoryLayout/MemoryLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <cstdint>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

/**
 * @brief This class is dervied from DynamicLayoutBuffer. As such, it implements the abstract methods and also implements pushRecord() and readRecord() as templated methods.
 * @caution This class is non-thread safe
 */
class RowLayoutTupleBuffer : public MemoryLayoutTupleBuffer {
  public:
    ~RowLayoutTupleBuffer() override = default;

    /**
     * @return retrieves the record size
     */
    FIELD_SIZE getRecordSize() { return dynamicRowLayout->getTupleSize(); }

    /**
     * @return retrieves the field offsets of the column layout
     */
    const std::vector<FIELD_SIZE>& getFieldOffSets() { return dynamicRowLayout->getFieldOffSets(); }

    /**
     * @param fieldName
     * @return field index from the fieldName
     */
    [[nodiscard]] std::optional<uint64_t> getFieldIndexFromName(std::string fieldName) const {
        return dynamicRowLayout->getFieldIndexFromName(std::move(fieldName));
    };

    /**
     * @brief Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer.
     * @tparam Types belonging to record
     * @param record
     * @tparam boundaryChecks if true will check if access is allowed
     * @return record at given recordIndex
     */
    template<bool boundaryChecks, typename... Types>
    std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * @brief Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * @tparam Types belonging to record
     * @tparam boundaryChecks if true will check if access is allowed
     * @param record
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

    /**
     * @brief Constructor for DynamicRowLayoutBuffer
     * @param tupleBuffer
     * @param capacity
     * @param dynamicRowLayout
     */
    RowLayoutTupleBuffer(TupleBuffer tupleBuffer, uint64_t capacity, std::shared_ptr<RowLayout> dynamicRowLayout);

  private:
    /**
     * @brief Copies fields of tuple sequentially to address, by iterating over tup via template recursion
     * @param address of corresponding tuples in tuple buffer
     * @param tup tuple to be read from
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup, uint8_t* address);

    /**
     * @brief Recursion anchor for above function
     * @param address of corresponding tuples in tuple buffer
     * @param tup tuple to be read from
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I == sizeof...(Ts)), void>::type copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                                                      const uint8_t* address);

    /**
     * @brief Copies fields of tuple sequentially from address, by iterating over tup via template recursion
     * @param address of corresponding tuples in tuple buffer
     * @param tup tuple to be written to
     * @tparam I works as a field index
     * @tparam Ts fields of tup
    */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I < sizeof...(Ts)), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, uint8_t* address);

    /**
     * @brief Recursion anchor for above function
     * @param address of corresponding tuples in tuple buffer
     * @param tup tuple to be read from
     * @tparam I works as a field index
     * @tparam Ts fields of tup
     */
    template<size_t I = 0, typename... Ts>
    typename std::enable_if<(I == sizeof...(Ts)), void>::type copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                                                                                        const uint8_t* address);

    const RowLayoutPtr dynamicRowLayout;
    uint8_t* basePointer;
};

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type RowLayoutTupleBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                                                                       uint8_t* address) {
    // Get current type of tuple and cast address to this type pointer
    *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address)) = std::get<I>(tup);

    // Go to the next field of tuple
    copyTupleFieldsToBuffer<I + 1>(tup, address + sizeof(typename std::tuple_element<I, std::tuple<Ts...>>::type));
}

template<size_t I, typename... Ts>
typename std::enable_if<(I == sizeof...(Ts)), void>::type RowLayoutTupleBuffer::copyTupleFieldsToBuffer(std::tuple<Ts...> tup,
                                                                                                        const uint8_t* address) {
    // Finished iterating through tuple via template recursion. So all that is left is to do a simple return.
    // As we are not using any variable, we need to have them set void otherwise the compiler will throw an unused variable error.
    ((void) address);
    ((void) tup);
}

template<size_t I, typename... Ts>
typename std::enable_if<(I == sizeof...(Ts)), void>::type
RowLayoutTupleBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup, const uint8_t* address) {
    // Iterated through tuple, so simply return
    ((void) address);
    ((void) tup);
}

template<size_t I, typename... Ts>
typename std::enable_if<(I < sizeof...(Ts)), void>::type RowLayoutTupleBuffer::copyTupleFieldsFromBuffer(std::tuple<Ts...>& tup,
                                                                                                         uint8_t* address) {
    // Get current type of tuple and cast address to this type pointer
    std::get<I>(tup) = *((typename std::tuple_element<I, std::tuple<Ts...>>::type*) (address));

    // Go to the next field of tuple
    copyTupleFieldsFromBuffer<I + 1>(tup, address + sizeof(typename std::tuple_element<I, std::tuple<Ts...>>::type));
}

template<bool boundaryChecks, typename... Types>
bool RowLayoutTupleBuffer::pushRecord(std::tuple<Types...> record) {
    // Calling pushRecord<>() with numberOfRecords as recordIndex.
    // This works as we are starting to count at 0 but numberOfRecords starts at 1
    // numberOfRecords will be increased by one in called function
    return pushRecord<boundaryChecks>(record, numberOfRecords);
}

template<bool boundaryChecks, typename... Types>
bool RowLayoutTupleBuffer::pushRecord(std::tuple<Types...> record, uint64_t recordIndex) {
    if (boundaryChecks && recordIndex >= capacity) {
        NES_WARNING("DynamicColumnLayoutBuffer: TupleBuffer is too small to write to position "
                    << recordIndex << " and thus no write can happen!");
        return false;
    }
    uint64_t offSet = (recordIndex * this->getRecordSize());
    uint8_t* address = basePointer + offSet;

    copyTupleFieldsToBuffer(record, address);

    if (recordIndex + 1 > numberOfRecords) {
        numberOfRecords = recordIndex + 1;
    }

    tupleBuffer.setNumberOfTuples(numberOfRecords);
    return true;
}

template<bool boundaryChecks, typename... Types>
std::tuple<Types...> RowLayoutTupleBuffer::readRecord(uint64_t recordIndex) {
    if (boundaryChecks && recordIndex >= capacity) {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutBuffer: Trying to access a record above capacity");
    }

    std::tuple<Types...> retTuple;

    uint64_t offSet = (recordIndex * this->getRecordSize());
    copyTupleFieldsFromBuffer(retTuple, basePointer + offSet);

    return retTuple;
}

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_BUFFER_HPP_
