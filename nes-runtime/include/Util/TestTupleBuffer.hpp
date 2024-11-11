/*
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

#ifndef NES_RUNTIME_INCLUDE_UTIL_TESTTUPLEBUFFER_HPP_
#define NES_RUNTIME_INCLUDE_UTIL_TESTTUPLEBUFFER_HPP_

#include <Common/ExecutableType/NESType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeUtil.hpp>
#include <Runtime/MemoryLayout/BufferAccessException.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
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
    explicit DynamicField(const uint8_t* address, PhysicalTypePtr physicalType);

    /**
     * @brief Read a pointer type and return the value as a pointer.
     * @tparam Type of the field requires to be a NesType and a pointer type.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     * @return Pointer type
     */
    template<class Type>
        requires IsNesType<Type> && std::is_pointer<Type>::value
    inline Type read() const {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        return reinterpret_cast<Type>(const_cast<uint8_t*>(address));
    };

    /**
     * @brief Reads a field with a value Type. Checks if the passed Type is the same as the physical field type.
     * @tparam Type of the field requires to be a NesType.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     * @return Value of the field.
     */
    template<class Type>
        requires(IsNesType<Type> && not std::is_pointer<Type>::value)
    inline Type& read() const {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        return *reinterpret_cast<Type*>(const_cast<uint8_t*>(address));
    };

    /**
     * @brief Reads a field with a value Type. Checks if the passed Type is the same as the physical field type.
     * @tparam Type of the field requires to be a NesType.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     * @return Value of the field.
     */
    template<class Type>
        requires(NESIdentifier<Type> && not std::is_pointer<Type>::value)
    inline Type read() const {
        if (!PhysicalTypes::isSamePhysicalType<typename Type::Underlying>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        return Type(*reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address)));
    };

    /**
     * @brief Writes a value to a specific field address.
     * @tparam Type of the field. Type has to be a NesType and to be compatible with the physical type of this field.
     * @param value of the field.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     */
    template<class Type>
        requires(IsNesType<Type>)
    inline void write(Type value) {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        *reinterpret_cast<Type*>(const_cast<uint8_t*>(address)) = value;
    };

    /**
     * @brief Writes a value to a specific field address.
     * @tparam Type of the field. Type has to be a NesType and to be compatible with the physical type of this field.
     * @param value of the field.
     * @throws BufferAccessException if the passed Type is not the same as the physicalType of the field.
     */
    template<class Type>
        requires(NESIdentifier<Type>)
    inline void write(Type value) {
        if (!PhysicalTypes::isSamePhysicalType<typename Type::Underlying>(physicalType)) {
            throw BufferAccessException("Wrong field type passed. Field is of type " + physicalType->toString()
                                        + " but accessed as " + typeid(Type).name());
        }
        *reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address)) = value.getRawValue();
    };

    /**
     * @brief get a string representation of this dynamic tuple
     * @return a string
     */
    std::string toString();

    /**
     * @brief Compares the two DynamicFields if there underlying memory is equal
     * @param rhs
     * @return True if equal otherwise false
     */
    [[nodiscard]] bool equal(const DynamicField& rhs) const;

    /**
     * @brief Checks if the DynamicField is equal
     * @param rhs
     * @return True if both fields are equal
     */
    bool operator==(const DynamicField& rhs) const;

    /**
     * @brief Checks if the DynamicField are not equal
     * @param rhs
     * @return True if both fields are equal
     */
    bool operator!=(const DynamicField& rhs) const;

    /**
     * @brief Getter for the physical type
     * @return Physical type
     */
    [[nodiscard]] const PhysicalTypePtr& getPhysicalType() const;

    /**
     * @brief Getter for the address
     * @return uint8_t
     */
    [[nodiscard]] const uint8_t* getAddressPointer() const;

  private:
    const uint8_t* address;
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
    DynamicField operator[](std::size_t fieldIndex) const;

    /**
    * @brief Accesses an individual field in the tuple by name.
    * @param field name
    * @throws BufferAccessException if field index is invalid
    * @return DynamicField
    */
    DynamicField operator[](std::string fieldName) const;

    /**
     * @brief Writes the variable sized value to this tuple at the fieldIndex
     * @param field: Can be either an index or the fieldName
     * @param value
     * @param bufferManager
     */
    void writeVarSized(std::variant<const uint64_t, const std::string> field, std::string value, BufferManager* bufferManager);

    /**
     * @brief Reads variable sized data and returns it as a string
     * @param field
     * @return VarSizedData as a string
     */
    std::string readVarSized(std::variant<const uint64_t, const std::string> field);

    /**
     * @brief get a string representation of this dynamic tuple
     * @return a string
     */
    std::string toString(const SchemaPtr& schema);

    /**
     * @brief Compares if the values of both tuples are equal. This means that the underlying memory layout CAN BE different
     * @param other
     * @return True, if equal otherwise false
     */
    bool operator==(const DynamicTuple& other) const;
    bool operator!=(const DynamicTuple& other) const;

  private:
    const uint64_t tupleIndex;
    const MemoryLayoutPtr memoryLayout;
    const TupleBuffer buffer;
};

/**
 * @brief The TestTupleBuffers allows to read records and individual fields from an tuple buffer.
 * To this end, it assumes a specific data layout, i.e., RowLayout or ColumnLayout.
 * This allows for dynamic accesses to a tuple buffer in the sense that at compile-time a user has not to specify a specific memory layout.
 * Therefore, the memory layout can be a runtime option, whereby the code that operates on the tuple buffer stays the same.
 * Furthermore, the TestTupleBuffers trades-off performance for safety.
 * To this end, it checks field bounds and field types and throws BufferAccessException if the passed parameters would lead to invalid buffer accesses.
 * The TestTupleBuffers supports different access methods:
 *
 *
 *    ```
 *    auto dBuffer = TestTupleBuffer(layout, buffer);
 *    auto value = dBuffer[tupleIndex][fieldIndex].read<uint_64>();
 *    ```
 *
 * #### Reading a specific field (F1) by name in a specific tuple:
 *    ```
 *    auto dBuffer = TestTupleBuffer(layout, buffer);
 *    auto value = dBuffer[tupleIndex]["F1"].read<uint_64>();
 *    ```
 *
 * #### Writing a specific field index in a specific tuple:
 *    ```
 *    auto dBuffer = TestTupleBuffer(layout, buffer);
 *    dBuffer[tupleIndex][fieldIndex].write<uint_64>(value);
 *    ```
 *
 * #### Iterating over all records in a tuple buffer:
 *    ```
 *    auto dBuffer = TestTupleBuffer(layout, buffer);
 *    for (auto tuple: dBuffer){
 *         auto value = tuple["F1"].read<uint_64>;
 *    }
 *    ```
 *
 * @caution This class is non-thread safe, i.e. multiple threads can manipulate the same tuple buffer at the same time.
 * @caution Do NOT use this class in performance critical code, as it is designed for testing and not for performance.
 */
class TestTupleBuffer {
  public:
    /**
     * @brief Constructor for TestTupleBuffer
     * @param memoryLayout memory layout to calculate field offset
     * @param tupleBuffer buffer that we want to access
     */
    explicit TestTupleBuffer(const MemoryLayoutPtr& memoryLayout, TupleBuffer buffer);

    /**
     * @brief Creates a TestTupleBuffer from the TupleBuffer and the schema
     * @param buffer
     * @param schema
     * @return TestTupleBuffer
     */
    static TestTupleBuffer createTestTupleBuffer(Runtime::TupleBuffer buffer, const SchemaPtr& schema);

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
   * @brief set the creation timestamp in milliseconds
   */
   void setCreationTimestampInMS(uint64_t value) const;

  /**
   * @brief set watermark to a tuple buffer
   */
  void setWatermark(uint64_t value) const;


    /**
   * @brief set the creation timestamp in milliseconds
   */
    void setCreationTimestampInMS(uint64_t value) const;

    /**
   * @brief set watermark to a tuple buffer
   */
    void setWatermark(uint64_t value) const;

    /**
     * @brief Accesses an individual tuple in the buffer.
     * @param tupleIndex the index of the record.
     * @throws BufferAccessException if index is larger then buffer capacity
     * @return DynamicRecord
     */
    DynamicTuple operator[](std::size_t tupleIndex) const;

    /**
     * @brief Gets the underling tuple buffer.
     * @return TupleBuffer
     */
    TupleBuffer getBuffer();

    /**
     * @brief Iterator to process the tuples in a TestTupleBuffer.
     * Take into account that it is invalid to add tuples to the tuple buffer while iterating over it.
     *    ```
     *    auto dBuffer = TestTupleBuffer(layout, buffer);
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
         * @param buffer the TestTupleBuffer that we want to process
         */
        explicit TupleIterator(const TestTupleBuffer& buffer);

        /**
         * @brief Constructor to create a new RecordIterator
         * @param buffer the TestTupleBuffer that we want to process
         * @param currentIndex the index of the current record
         */
        explicit TupleIterator(const TestTupleBuffer& buffer, const uint64_t currentIndex);

        /**
         * @brief Copy Constructor
         * @param other
         */
        TupleIterator(const TupleIterator& other);

        TupleIterator& operator++();
        const TupleIterator operator++(int);
        bool operator==(TupleIterator other) const;
        bool operator!=(TupleIterator other) const;
        reference operator*() const;

      private:
        const TestTupleBuffer& buffer;
        uint64_t currentIndex;
    };

    /**
     * @brief Start of the iterator at index 0.
     * @return TupleIterator
     */
    TupleIterator begin() const;

    /**
     * @brief End of the iterator at index getNumberOfTuples().
     * @return TupleIterator
     */
    TupleIterator end() const;

    /**
     * @brief Outputs the content of a tuple buffer to a output stream.
     * @param os output stream
     * @param buffer dynamic tupleBuffer
     * @return result stream
     */
    friend std::ostream& operator<<(std::ostream& os, const TestTupleBuffer& buffer);

    /**
     * @brief Creates a string representation of the dynamic tuple buffer
     * @return a string representation
     */
    std::string toString(const SchemaPtr& schema, bool showHeader = true);

    /**
     * @brief Push a record to the underlying tuple buffer. Simply appends record to the end of the buffer.  
             Boundary checks are performed by the write function of the TestTupleBuffer.
     * @note Recursive templates have a limited depth. The recommended (C++ standard) depth is 1024.
     *       Thus, a record with more than 1024 fields might not be supported.
     * @param record: The record to be pushed to the buffer.
     */
    template<typename... Types>
        requires(!ContainsString<Types> && ...)
    void pushRecordToBuffer(std::tuple<Types...> record) {
        pushRecordToBufferAtIndex(record, buffer.getNumberOfTuples());
    }

    /**
     * @brief Push a record to the underlying tuple buffer. Simply appends record to the end of the buffer.
              Boundary checks are performed by the write function of the TestTupleBuffer.
     * @note  Recursive templates have a limited depth. The recommended (C++ standard) depth is 1024.
              Thus, a record with more than 1024 fields might not be supported.
     * @param record: The record to be pushed to the buffer.
     * @param bufferManager: BufferManager required for storing the variable sized data in the child buffers
     */
    template<typename... Types>
        requires(ContainsString<Types> || ...)
    void pushRecordToBuffer(std::tuple<Types...> record, BufferManager* bufferManager) {
        pushRecordToBufferAtIndex(record, buffer.getNumberOfTuples(), bufferManager);
    }

    /**
     * @brief Push a record to the underlying tuple buffer at given recordIndex. Boundary checks are performed by the 
                write function of the TestTupleBuffer.
     * @note Recursive templates have a limited depth. The recommended (C++ standard) depth is 1024.
     *       Thus, a record with more than 1024 fields might not be supported.
     *
     * @param record: The record to be pushed to the buffer.
     * @param recordIndex: The index at which the record should be pushed to the buffer.
     * @return true if the record was pushed successfully, false otherwise.
     */
    template<typename... Types>
    void pushRecordToBufferAtIndex(std::tuple<Types...> record, uint64_t recordIndex, BufferManager* bufferManager = nullptr) {
        uint64_t numberOfRecords = buffer.getNumberOfTuples();
        uint64_t fieldIndex = 0;
        if (recordIndex >= buffer.getBufferSize()) {
            throw BufferAccessException("Current buffer is not big enough for index. Current buffer size: "
                                        + std::to_string(buffer.getBufferSize()) + ", Index: " + std::to_string(recordIndex));
        }
        // std::apply allows us to iterate over a tuple (with template recursion) with a lambda function.
        // On each iteration, the lambda function is called with the current field value, and the field index is increased.
        // If the value is a std::string, we call writeVarSized() instead of write().
        std::apply(
            [&](auto&&... fieldValue) {
                (([&]() {
                     if constexpr (IsString<decltype(fieldValue)>) {
                         NES_ASSERT(bufferManager != nullptr, "BufferManager can not be null while using variable sized data!");
                         (*this)[recordIndex].writeVarSized(fieldIndex++, fieldValue, bufferManager);
                     } else {
                         (*this)[recordIndex][fieldIndex++].write(fieldValue);
                     }
                 }()),
                 ...);
            },
            record);
        if (recordIndex + 1 > numberOfRecords) {
            this->setNumberOfTuples(recordIndex + 1);
        }
    }

    /**
     * @brief Copy a record from the underlying tuple buffer to a tuple. Boundary checks are performed by the 
                read function of the TestTupleBuffer.
     * 
     * @param recordIndex: The index of the record to be copied.
     * @return std::tuple<Types...> The indexed record represented as a std:tuple.
     * @return true if the record was read from the TupleBuffer successfully, false otherwise.
     */
    template<typename... Types>
    std::tuple<Types...> readRecordFromBuffer(uint64_t recordIndex) {
        NES_ASSERT((sizeof...(Types)) == memoryLayout->getFieldSizes().size(),
                   "Provided tuple types: " << sizeof...(Types) << " do not match the number of fields in the memory layout: "
                                            << memoryLayout->getFieldSizes().size() << '\n');
        std::tuple<Types...> retTuple;
        copyRecordFromBufferToTuple(retTuple, recordIndex);
        return retTuple;
    }

    /**
     * @brief Returns the number of occurrences of the tuple in this buffer
     * @param tuple
     * @return Count of occurrences
     */
    uint64_t countOccurrences(DynamicTuple& tuple) const;

    /**
     * @brief Gets the memoryLayout.
     * @return MemoryLayoutPtr
     */
    MemoryLayoutPtr getMemoryLayout() const;

  private:
    /**
     * @brief Takes a tuple as a reference and a recordIndex. Copies the record in the TupleBuffer at the given 
                recordIndex to the tuple.
     * @note Recursive templates have a limited depth. The recommended (C++ standard) depth is 1024.
     *       Thus, a record with more than 1024 fields might not be supported.
     *
     * @param record: The record to be pushed to the buffer.
     * @param recordIndex: The index at which the record should be pushed to the buffer.
     * @return true if the record was read from the TupleBuffer successfully, false otherwise.
     */
    template<size_t I = 0, typename... Types>
    void copyRecordFromBufferToTuple(std::tuple<Types...>& record, uint64_t recordIndex) {
        // Check if I matches the size of the tuple, which means that all fields of the record have been processed.
        if constexpr (I != sizeof...(Types)) {
            if constexpr (IsString<typename std::tuple_element<I, std::tuple<Types...>>::type>) {
                auto childBufferIdx = (*this)[recordIndex][I].read<TupleBuffer::NestedTupleBufferKey>();
                std::get<I>(record) = readVarSizedData(this->buffer, childBufferIdx);
            } else {
                // Get type of current tuple element and cast field value to this type. Add value to return tuple.
                std::get<I>(record) =
                    ((*this)[recordIndex][I]).read<typename std::tuple_element<I, std::tuple<Types...>>::type>();
            }

            // Recursive call to copyRecordFromBufferToTuple with the field index (I) increased by 1.
            copyRecordFromBufferToTuple<I + 1>(record, recordIndex);
        }
    }

  private:
    const MemoryLayoutPtr memoryLayout;
    mutable TupleBuffer buffer;
};

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_RUNTIME_INCLUDE_UTIL_TESTTUPLEBUFFER_HPP_
