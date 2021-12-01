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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_MAGIC_LAYOUT_BUFFER_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_MAGIC_LAYOUT_BUFFER_HPP_

#include <Common/ExecutableType/NESType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeUtil.hpp>
#include <Exceptions/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstring>
#include <ostream>
#include <variant>

namespace NES::Runtime::MemoryLayouts {

class MemoryLayoutTupleBuffer;
using MemoryLayoutBufferPtr = std::shared_ptr<MemoryLayoutTupleBuffer>;

/**
 * @brief The DynamicField allows to read and write a field at a
 * specific address and a specific data type.
 * For all field accesses we check that the template type is the same as the selected physical field type.
 * If the type is not compatible accesses result in a BufferAccessException.
 */
class DynamicField {
  public:
    /**
     * @brief Constructor to create a DynamicField
     * @param address for the field
     */
    explicit DynamicField(uint8_t* address, PhysicalTypePtr physicalType);

    /**
     * @brief Read a pointer type and return the value as a pointer.
     * @tparam Type of the field requires to be a NesType and a pointer type.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     * @return Pointer type
     */
    template<class Type>
    requires IsNesType<Type> && std::is_pointer<Type>::value inline Type read() const {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        return reinterpret_cast<Type>(address);
    };

    /**
     * @brief Reads a field with a value Type. Checks if the passed Type is the same as the physical field type.
     * @tparam Type of the field requires to be a NesType.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     * @return Value of the field.
     */
    template<class Type>
    requires(IsNesType<Type> && not std::is_pointer<Type>::value) inline Type& read() const {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        return *reinterpret_cast<Type*>(address);
    };

    /**
     * @brief Writes a value to a specific field address.
     * @tparam Type of the field. Type has to be a NesType and to be compatible with the physical type of this field.
     * @param value of the field.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     */
    template<class Type>
    requires(IsNesType<Type>) inline void write(Type value) {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        *reinterpret_cast<Type*>(address) = value;
    };

  private:
    uint8_t* address;
    const PhysicalTypePtr physicalType;
};

/**
 * @brief The DynamicRecords allows to read individual fields of a tuple.
 * Field accesses are safe in the sense that if is checked the field exists.
 */
class DynamicTuple {
  public:
    /**
     * @brief Constructor for the DynamicTuple.
     * Each tuple contains the index, to the memory layout and to the tuple buffer.
     * @param tupleIndex
     * @param memoryLayout
     * @param buffer
     */
    DynamicTuple(uint64_t tupleIndex, MemoryLayoutPtr memoryLayout, TupleBuffer buffer);

    /**
     * @brief Accesses an individual field in the tuple by index.
     * @param fieldIndex
     * @throws BufferAccessException if field index is invalid
     * @return DynamicField
     */
    DynamicField operator[](std::size_t fieldIndex);

    /**
    * @brief Accesses an individual field in the tuple by name.
    * @param field name
    * @throws BufferAccessException if field index is invalid
    * @return DynamicField
    */
    DynamicField operator[](std::string fieldName);

  private:
    const uint64_t tupleIndex;
    const MemoryLayoutPtr memoryLayout;
    TupleBuffer buffer;
};

/**
 * @brief The DynamicTupleBuffers allows to read records and individual fields from an tuple buffer.
 * To this end, it assumes a specific data layout, i.e., RowLayout or ColumnLayout.
 * This allows for dynamic accesses to a tuple buffer in the sense that at compile-time a user has not to specify a specific memory layout.
 * Therefore, the memory layout can be a runtime option, whereby the code that operates on the tuple buffer stays the same.
 * Furthermore, the DynamicTupleBuffers trades-off performance for safety.
 * To this end, it checks field bounds and field types and throws BufferAccessException if the passed parameters would lead to invalid buffer accesses.
 * The DynamicTupleBuffers supports different access methods:
 *
 *
 *    ```
 *    auto dBuffer = DynamicTupleBuffer(layout, buffer);
 *    auto value = dBuffer[tupleIndex][fieldIndex].read<uint_64>();
 *    ```
 *
 * #### Reading a specific field (F1) by name in a specific tuple:
 *    ```
 *    auto dBuffer = DynamicTupleBuffer(layout, buffer);
 *    auto value = dBuffer[tupleIndex]["F1"].read<uint_64>();
 *    ```
 *
 * #### Writing a specific field index in a specific tuple:
 *    ```
 *    auto dBuffer = DynamicTupleBuffer(layout, buffer);
 *    dBuffer[tupleIndex][fieldIndex].write<uint_64>(value);
 *    ```
 *
 * #### Iterating over all records in a tuple buffer:
 *    ```
 *    auto dBuffer = DynamicTupleBuffer(layout, buffer);
 *    for (auto tuple: dBuffer){
 *         auto value = tuple["F1"].read<uint_64>;
 *    }
 *    ```
 *
 * @caution This class is non-thread safe, i.e. multiple threads can manipulate the same tuple buffer at the same time.
 */
class DynamicTupleBuffer {
  public:
    /**
     * @brief Constructor for DynamicTupleBuffer
     * @param memoryLayout memory layout to calculate field offset
     * @param tupleBuffer buffer that we want to access
     */
    explicit DynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);

    /**
    * @brief Gets the number of tuples a tuple buffer with this memory layout could occupy.
    * @return number of tuples a tuple buffer can occupy.
    */
    [[nodiscard]] uint64_t getCapacity() const;

    /**
     * @brief Gets the current number of tuples that are currently stored in the underling tuple buffer
     * @return Number of tuples that are in the associated buffer
     */
    [[nodiscard]] uint64_t getNumberOfTuples() const;

    /**
     * @brief Set the number of records to the underling tuple buffer.
     */
    void setNumberOfTuples(uint64_t value);

    /**
     * @brief Accesses an individual tuple in the buffer.
     * @param tupleIndex the index of the record.
     * @throws BufferAccessException if index is larger then buffer capacity
     * @return DynamicRecord
     */
    DynamicTuple operator[](std::size_t tupleIndex);

    /**
     * @brief Gets the underling tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer getBuffer();

    /**
     * @brief Iterator to process the tuples in a DynamicTupleBuffer.
     * Take into account that it is invalid to add tuples to the tuple buffer while iterating over it.
     *    ```
     *    auto dBuffer = DynamicTupleBuffer(layout, buffer);
     *    for (auto tuple: dBuffer){
     *         auto value = tuple["F1"].read<uint_64>;
     *    }
     *    ```
     */
    class TupleIterator : public std::iterator<std::input_iterator_tag,// iterator_category
                                               DynamicTuple,           // value_type
                                               DynamicTuple,           // difference_type
                                               DynamicTuple*,          // pointer
                                               DynamicTuple            // reference
                                               > {
      public:
        /**
         * @brief Constructor to create a new TupleIterator
         * @param buffer the DynamicTupleBuffer that we want to process
         */
        explicit TupleIterator(DynamicTupleBuffer& buffer);

        /**
         * @brief Constructor to create a new RecordIterator
         * @param buffer the DynamicTupleBuffer that we want to process
         * @param currentIndex the index of the current record
         */
        explicit TupleIterator(DynamicTupleBuffer& buffer, uint64_t currentIndex);

        TupleIterator& operator++();
        const TupleIterator operator++(int);
        bool operator==(TupleIterator other) const;
        bool operator!=(TupleIterator other) const;
        reference operator*() const;

      private:
        DynamicTupleBuffer& buffer;
        uint64_t currentIndex;
    };

    /**
     * @brief Start of the iterator at index 0.
     * @return TupleIterator
     */
    TupleIterator begin();

    /**
     * @brief End of the iterator at index getNumberOfTuples().
     * @return TupleIterator
     */
    TupleIterator end();

    /**
     * @brief Outputs the content of a tuple buffer to a output stream.
     * @param os output stream
     * @param buffer dynamic tupleBuffer
     * @return result stream
     */
    friend std::ostream& operator<<(std::ostream& os, const DynamicTupleBuffer& buffer);

  private:
    const MemoryLayoutPtr memoryLayout;
    mutable TupleBuffer buffer;
};

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_MAGIC_LAYOUT_BUFFER_HPP_
