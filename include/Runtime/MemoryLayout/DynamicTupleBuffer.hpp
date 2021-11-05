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

using FIELD_SIZE = uint64_t;

class MemoryLayoutTupleBuffer;
using DynamicLayoutBufferPtr = std::shared_ptr<MemoryLayoutTupleBuffer>;



class DynamicField {
  public:
    explicit DynamicField(uint8_t* address);
    template<class Type>
    requires IsNesType<Type> && std::is_pointer<Type>::value inline Type read() const { return reinterpret_cast<Type>(address); };

    template<class Type>
    requires(IsNesType<Type> && not std::is_pointer<Type>::value) inline Type& read() const {
        return *reinterpret_cast<Type*>(address);
    };

    template<class Type>
    inline void write(Type value) {
        *reinterpret_cast<Type*>(address) = value;
    };

  private:
    uint8_t* address;
};

class DynamicRecord {
  public:
    DynamicRecord(uint64_t recordIndex, DynamicLayoutBufferPtr buffer);
    DynamicField operator[](std::size_t fieldIndex) const;

  private:
    uint64_t recordIndex;
    DynamicLayoutBufferPtr buffer;
};

/**
 * @brief This abstract class is the base class for DynamicRowLayoutBuffer and DynamicColumnLayoutBuffer.
 * As the base class, it has multiple methods or members that are useful for both derived classes.
 * @caution This class is non-thread safe
 */
class DynamicTupleBuffer {
  public:
    /**
     * @brief Constructor for DynamicLayoutBuffer
     * @param tupleBuffer
     * @param capacity
     */
    explicit DynamicTupleBuffer(std::shared_ptr<MemoryLayoutTupleBuffer> buffer);

    virtual ~DynamicTupleBuffer() = default;

    /**
    * @brief This method returns the maximum number of records, so the capacity.
    * @return
    */
    [[nodiscard]] uint64_t getCapacity() const;

    /**
     * @brief This method returns the current number of records that are in the associated buffer
     * @return
     */
    [[nodiscard]] uint64_t getNumberOfRecords() const;

    DynamicRecord operator[](std::size_t recordIndex);

    // member typedefs provided through inheriting from std::iterator
    class RecordIterator : public std::iterator<std::input_iterator_tag,// iterator_category
                                                DynamicRecord,          // value_type
                                                DynamicRecord,          // difference_type
                                                DynamicRecord*,         // pointer
                                                DynamicRecord           // reference
                                                > {
      public:
        explicit RecordIterator(DynamicTupleBuffer& buffer);
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
    RecordIterator begin() { return RecordIterator(*this); }
    RecordIterator end() { return RecordIterator(*this, getNumberOfRecords()); }

  private:
    std::shared_ptr<MemoryLayoutTupleBuffer> buffer;
};

}// namespace NES::Runtime::DynamicMemoryLayout

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_MAGIC_LAYOUT_BUFFER_HPP_
