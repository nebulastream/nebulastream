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

#include <QueryCompiler/GeneratableTypes/NESType.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cstring>
#include <variant>

namespace NES::Runtime::MemoryLayouts {

class MemoryLayoutTupleBuffer;
using MemoryLayoutBufferPtr = std::shared_ptr<MemoryLayoutTupleBuffer>;

/**
 * @brief The DynamicField allows to read and write a field at a
 * specific address and a specific data type.
 */
class DynamicField {
  public:
    /**
     * @brief Constructor to create a DynamicField
     * @param address for the field
     */
    explicit DynamicField(uint8_t* address);

    /**
     * @brief Read a pointer type and return the value as a pointer.
     * @tparam Type of the field requires to be a NesType and a pointer type.
     * @return Pointer type
     */
    template<class Type>
    requires IsNesType<Type> && std::is_pointer<Type>::value inline Type read() const { return reinterpret_cast<Type>(address); };

    /**
     * @brief Reads a field with a specific pointer.
     * @tparam Type of the field requires to be a NesType.
     * @return Value of the field.
     */
    template<class Type>
    requires(IsNesType<Type> && not std::is_pointer<Type>::value) inline Type& read() const {
        return *reinterpret_cast<Type*>(address);
    };

    /**
     * @brief Writes a value to a specific field.
     * @tparam Type of the field. Type has to be a NesType.
     * @param value of the field.
     */
    template<class Type>
    requires(IsNesType<Type>) inline void write(Type value) { *reinterpret_cast<Type*>(address) = value; };

  private:
    uint8_t* address;
};

/**
 * @brief The DynamicRecords allows to read individual fields of a record
 */
class DynamicTuple {
  public:
    /**
     * @brief Constructor for the DynamicRecord.
     * Each record contains the index, to the memory layout and to the tuple buffer.
     * @param recordIndex
     * @param memoryLayout
     * @param buffer
     */
    DynamicTuple(uint64_t recordIndex,
                  MemoryLayoutPtr  memoryLayout,
                  TupleBuffer& buffer);

    /**
     * @brief Accesses an individual field in the record.
     * @param fieldIndex
     * @throws BufferAccessException if field index is larger then buffer capacity
     * @return DynamicField
     */
    DynamicField operator[](std::size_t fieldIndex);

  private:
    const uint64_t recordIndex;
    const MemoryLayoutPtr memoryLayout;
    TupleBuffer buffer;
};

/**
 * @brief The DynamicTupleBuffers allows to read records and individual fields from an tuple buffer.
 * To this end, it assumes a specific data layout, i.e., RowLayout or ColumnLayout.
 * @caution This class is non-thread safe, i.e. multiple threads can manipulate the same tuple buffer at the same time.
 */
class DynamicTupleBuffer {
  public:
    /**
     * @brief Constructor for DynamicTupleBuffer
     * @param memoryLayout memory layout to calculate field offset
     * @param tupleBuffer buffer that we want to access
     */
    explicit DynamicTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer& buffer);

    /**
    * @brief This method returns the maximum number of records, so the capacity.
    * @return capacity of the buffer.
    */
    [[nodiscard]] uint64_t getCapacity() const;

    /**
     * @brief This method returns the current number of records that are in the associated buffer
     * @return Number of records that are in the associated buffer
     */
    [[nodiscard]] uint64_t getNumberOfRecords() const;

    /**
     * @brief Set the number of records to the TupleBuffer
     */
    void setNumberOfRecords(uint64_t value);

    /**
     * @brief Accesses an individual record in the buffer
     * @param recordIndex the index of the record
     * @throws BufferAccessException if index is larger then buffer capacity
     * @return DynamicRecord
     */
    DynamicTuple operator[](std::size_t recordIndex);

    /**
     * @brief Iterator to process the records in a DynamicTupleBuffer
     */
    class RecordIterator : public std::iterator<std::input_iterator_tag,// iterator_category
                                                DynamicTuple,          // value_type
                                                DynamicTuple,          // difference_type
                                                DynamicTuple*,         // pointer
                                                DynamicTuple            // reference
                                                > {
      public:
        /**
         * @brief Constructor to create a new RecordIterator
         * @param buffer the DynamicTupleBuffer that we want to process
         */
        explicit RecordIterator(DynamicTupleBuffer& buffer);

        /**
         * @brief Constructor to create a new RecordIterator
         * @param buffer the DynamicTupleBuffer that we want to process
         * @param currentIndex the index of the current record
         */
        explicit RecordIterator(DynamicTupleBuffer& buffer, uint64_t currentIndex);

        RecordIterator& operator++();
        const RecordIterator operator++(int);
        bool operator==(RecordIterator other) const;
        bool operator!=(RecordIterator other) const;
        reference operator*() const;

      private:
        DynamicTupleBuffer& buffer;
        uint64_t currentIndex;
    };

    /**
     * @brief Start of the iterator at index 0
     * @return RecordIterator
     */
    RecordIterator begin() { return RecordIterator(*this); }

    /**
     * @brief End of the iterator at index getNumberOfRecords()
     * @return RecordIterator
     */
    RecordIterator end() { return RecordIterator(*this, getNumberOfRecords()); }

  private:
    const MemoryLayoutPtr memoryLayout;
    TupleBuffer buffer;
};

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_MAGIC_LAYOUT_BUFFER_HPP_
