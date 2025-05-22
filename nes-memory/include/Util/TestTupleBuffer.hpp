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

#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <variant>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalTypeUtil.hpp>

namespace NES::Memory::MemoryLayouts
{

template <class Type>
concept IsNesType = std::is_fundamental_v<Type> || std::is_fundamental_v<std::remove_pointer_t<Type>>;


/// This concept checks via tuple unpacking if Types contains at least one string.
template <class... Types>
concept ContainsString = requires { requires(std::is_same_v<std::string, Types> || ...); };

template <class Type>
concept IsString = std::is_same_v<std::remove_cvref_t<Type>, std::string>;

/// The DynamicField allows to read and write a field at a
/// specific address and a specific data type.
/// For all field accesses we check that the template type is the same as the selected physical field type.
/// If the type is not compatible accesses result in a CannotAccessBuffer.
class DynamicField
{
public:
    explicit DynamicField(const uint8_t* address, std::shared_ptr<PhysicalType> physicalType);

    /// Read a pointer type and return the value as a pointer.
    /// @tparam Type of the field requires to be a NesType.
    /// @throws CannotAccessBuffer if the passed Type is not the same as the physicalType of the field.
    template <class Type>
    requires IsNesType<Type> && std::is_pointer<Type>::value
    [[nodiscard]] Type read() const
    {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType))
        {
            throw CannotAccessBuffer(
                "Wrong field type passed. Field is of type {} but accessed as {}", physicalType->toString(), typeid(Type).name());
        }
        return reinterpret_cast<Type>(const_cast<uint8_t*>(address));
    };

    /// @brief Reads a field with a value Type. Checks if the passed Type is the same as the physical field type.
    /// @tparam Type of the field requires to be a NesType.
    /// @throws CannotAccessBuffer if the passed Type is not the same as the physicalType of the field.
    /// @return Value of the field.
    template <class Type>
    requires(IsNesType<Type> && not std::is_pointer<Type>::value)
    [[nodiscard]] Type& read() const
    {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType))
        {
            throw CannotAccessBuffer(
                "Wrong field type passed. Field is of type {} but accessed as {}", physicalType->toString(), typeid(Type).name());
        }
        return *reinterpret_cast<Type*>(const_cast<uint8_t*>(address));
    };

    /// @brief Reads a field with a value Type. Checks if the passed Type is the same as the physical field type.
    /// @tparam Type of the field requires to be a NesType.
    /// @throws CannotAccessBuffer if the passed Type is not the same as the physicalType of the field.
    /// @return Value of the field.
    template <class Type>
    requires(NESIdentifier<Type> && not std::is_pointer<Type>::value)
    inline Type read() const
    {
        if (!PhysicalTypes::isSamePhysicalType<typename Type::Underlying>(physicalType))
        {
            throw CannotAccessBuffer(
                "Wrong field type passed. Field is of type {} but accessed as {}", physicalType->toString(), typeid(Type).name());
        }
        return Type(*reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address)));
    };

    /// @brief Writes a value to a specific field address.
    /// @tparam Type of the field. Type has to be a NesType and to be compatible with the physical type of this field.
    /// @param value of the field.
    /// @throws CannotAccessBuffer if the passed Type is not the same as the physicalType of the field.
    template <class Type>
    requires(IsNesType<Type>)
    void write(Type value)
    {
        if (!PhysicalTypes::isSamePhysicalType<Type>(physicalType))
        {
            throw CannotAccessBuffer(
                "Wrong field type passed. Field is of type {} but accessed as {}", physicalType->toString(), typeid(Type).name());
        }
        *reinterpret_cast<Type*>(const_cast<uint8_t*>(address)) = value;
    };

    /// @brief Writes a value to a specific field address.
    /// @tparam Type of the field. Type has to be a NesType and to be compatible with the physical type of this field.
    /// @param value of the field.
    /// @throws CannotAccessBuffer if the passed Type is not the same as the physicalType of the field.
    template <class Type>
    requires(NESIdentifier<Type>)
    void write(Type value)
    {
        if (!PhysicalTypes::isSamePhysicalType<typename Type::Underlying>(physicalType))
        {
            throw CannotAccessBuffer(
                "Wrong field type passed. Field is of type {} but accessed as {}", physicalType->toString(), typeid(Type).name());
        }
        *reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address)) = value.getRawValue();
    };

    [[nodiscard]] std::string toString() const;

    /// @brief Compares the two DynamicFields if there underlying memory is equal
    [[nodiscard]] bool equal(const DynamicField& rhs) const;

    /// @brief Checks if the DynamicField is equal
    bool operator==(const DynamicField& rhs) const;

    bool operator!=(const DynamicField& rhs) const;

    [[nodiscard]] const std::shared_ptr<PhysicalType>& getPhysicalType() const;

    [[nodiscard]] const uint8_t* getAddressPointer() const;

private:
    const uint8_t* address;
    std::shared_ptr<PhysicalType> physicalType;
};

/// The DynamicRecords allows to read individual fields of a tuple.
/// Field accesses are safe in the sense that if is checked the field exists.
class DynamicTuple
{
public:
    /// Each tuple contains the index, to the memory layout and to the tuple buffer.
    DynamicTuple(uint64_t tupleIndex, std::shared_ptr<MemoryLayout> memoryLayout, Memory::TupleBuffer buffer);

    /// @throws CannotAccessBuffer if field index is invalid
    DynamicField operator[](std::size_t fieldIndex) const;


    /// @throws CannotAccessBuffer if field index is invalid
    DynamicField operator[](std::string fieldName) const;

    void
    writeVarSized(std::variant<const uint64_t, const std::string> field, std::string value, Memory::AbstractBufferProvider& bufferProvider);

    std::string readVarSized(std::variant<const uint64_t, const std::string> field);

    [[nodiscard]] std::string toString(const std::shared_ptr<Schema>& schema) const;

    /// Compares if the values of both tuples are equal.
    /// @note This means that the underlying memory layout CAN BE different
    bool operator==(const DynamicTuple& other) const;
    bool operator!=(const DynamicTuple& other) const;

private:
    uint64_t tupleIndex;
    std::shared_ptr<MemoryLayout> memoryLayout;
    Memory::TupleBuffer buffer;
};

/**
 * @brief The TestTupleBuffers allows to read records and individual fields from an tuple buffer.
 * To this end, it assumes a specific data layout, i.e., RowLayout or ColumnLayout.
 * This allows for dynamic accesses to a tuple buffer in the sense that at compile-time a user has not to specify a specific memory layout.
 * Therefore, the memory layout can be a runtime option, whereby the code that operates on the tuple buffer stays the same.
 * Furthermore, the TestTupleBuffers trades-off performance for safety.
 * To this end, it checks field bounds and field types and throws CannotAccessBuffer if the passed parameters would lead to invalid buffer accesses.
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
class TestTupleBuffer
{
public:
    enum class PrintMode : uint8_t
    {
        SHOW_HEADER_END_IN_NEWLINE,
        SHOW_HEADER_END_WITHOUT_NEWLINE,
        NO_HEADER_END_IN_NEWLINE,
        NO_HEADER_END_WITHOUT_NEWLINE,
    };
    explicit TestTupleBuffer(const std::shared_ptr<MemoryLayout>& memoryLayout, Memory::TupleBuffer buffer);

    static TestTupleBuffer createTestTupleBuffer(const Memory::TupleBuffer& buffer, const std::shared_ptr<Schema>& schema);

    /// Gets the number of tuples a tuple buffer with this memory layout could occupy.
    [[nodiscard]] uint64_t getCapacity() const;

    /// Gets the current number of tuples that are currently stored in the underling tuple buffer
    [[nodiscard]] uint64_t getNumberOfTuples() const;

    void setNumberOfTuples(uint64_t value);


    /// @throws CannotAccessBuffer if index is larger then buffer capacity
    DynamicTuple operator[](std::size_t tupleIndex) const;

    Memory::TupleBuffer getBuffer();

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
    class TupleIterator : public std::iterator<
                              std::input_iterator_tag, /// iterator_category
                              DynamicTuple, /// value_type
                              DynamicTuple, /// difference_type
                              DynamicTuple*, /// pointer
                              DynamicTuple /// reference
                              >
    {
    public:
        explicit TupleIterator(const TestTupleBuffer& buffer);

        explicit TupleIterator(const TestTupleBuffer& buffer, const uint64_t currentIndex);

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

    /// Start of the iterator at index 0.
    TupleIterator begin() const;

    /// End of the iterator at index getNumberOfTuples().
    TupleIterator end() const;

    friend std::ostream& operator<<(std::ostream& os, const TestTupleBuffer& buffer);

    std::string toString(const std::shared_ptr<Schema>& schema) const;
    std::string toString(const std::shared_ptr<Schema>& schema, PrintMode printMode) const;

    /**
     * @brief Push a record to the underlying tuple buffer. Simply appends record to the end of the buffer.  
             Boundary checks are performed by the write function of the TestTupleBuffer.
     * @note Recursive templates have a limited depth. The recommended (C++ standard) depth is 1024.
     *       Thus, a record with more than 1024 fields might not be supported.
     * @param record: The record to be pushed to the buffer.
     */
    template <typename... Types>
    requires(!ContainsString<Types> && ...)
    void pushRecordToBuffer(std::tuple<Types...> record)
    {
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
    template <typename... Types>
    requires(ContainsString<Types> || ...)
    void pushRecordToBuffer(std::tuple<Types...> record, Memory::BufferManager* bufferManager)
    {
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
     * @throws CannotAccessBuffer if the recordIndex is outside the buffer
     * @return true if the record was pushed successfully, false otherwise.
     */
    template <typename... Types>
    void
    pushRecordToBufferAtIndex(std::tuple<Types...> record, uint64_t recordIndex, Memory::AbstractBufferProvider* bufferProvider = nullptr)
    {
        uint64_t numberOfRecords = buffer.getNumberOfTuples();
        uint64_t fieldIndex = 0;
        if (recordIndex >= buffer.getBufferSize())
        {
            throw CannotAccessBuffer(
                "Current buffer is not big enough for index. Current buffer size: " + std::to_string(buffer.getBufferSize())
                + ", Index: " + std::to_string(recordIndex));
        }
        /// std::apply allows us to iterate over a tuple (with template recursion) with a lambda function.
        /// On each iteration, the lambda function is called with the current field value, and the field index is increased.
        /// If the value is a std::string, we call writeVarSized() instead of write().
        std::apply(
            [&](auto&&... fieldValue)
            {
                ((
                     [&]()
                     {
                         if constexpr (IsString<decltype(fieldValue)>)
                         {
                             INVARIANT(bufferProvider != nullptr, "BufferManager can not be null while using variable sized data!");
                             (*this)[recordIndex].writeVarSized(fieldIndex++, fieldValue, *bufferProvider);
                         }
                         else
                         {
                             (*this)[recordIndex][fieldIndex++].write(fieldValue);
                         }
                     }()),
                 ...);
            },
            record);
        if (recordIndex + 1 > numberOfRecords)
        {
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
    template <typename... Types>
    std::tuple<Types...> readRecordFromBuffer(uint64_t recordIndex)
    {
        PRECONDITION(
            (sizeof...(Types)) == memoryLayout->getSchema()->getFieldCount(),
            "Provided tuple types: {} do not match the number of fields in the memory layout: {}",
            sizeof...(Types),
            memoryLayout->getSchema()->getFieldCount());
        std::tuple<Types...> retTuple;
        copyRecordFromBufferToTuple(retTuple, recordIndex);
        return retTuple;
    }

    uint64_t countOccurrences(DynamicTuple& tuple) const;

    std::shared_ptr<MemoryLayout> getMemoryLayout() const;

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
    template <size_t I = 0, typename... Types>
    void copyRecordFromBufferToTuple(std::tuple<Types...>& record, uint64_t recordIndex)
    {
        /// Check if I matches the size of the tuple, which means that all fields of the record have been processed.
        if constexpr (I != sizeof...(Types))
        {
            if constexpr (IsString<typename std::tuple_element<I, std::tuple<Types...>>::type>)
            {
                auto childBufferIdx = (*this)[recordIndex][I].read<Memory::TupleBuffer::NestedTupleBufferKey>();
                std::get<I>(record) = readVarSizedData(this->buffer, childBufferIdx);
            }
            else
            {
                /// Get type of current tuple element and cast field value to this type. Add value to return tuple.
                std::get<I>(record) = ((*this)[recordIndex][I]).read<typename std::tuple_element<I, std::tuple<Types...>>::type>();
            }

            /// Recursive call to copyRecordFromBufferToTuple with the field index (I) increased by 1.
            copyRecordFromBufferToTuple<I + 1>(record, recordIndex);
        }
    }

    std::shared_ptr<MemoryLayout> memoryLayout;
    Memory::TupleBuffer buffer;
};

}
