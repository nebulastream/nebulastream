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

#include <Runtime/TupleBuffer.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <variant>
#include <span>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

template <class Type>
concept IsNesType = std::is_fundamental_v<Type> || std::is_fundamental_v<std::remove_pointer_t<Type>>;


template <class... Types>
concept ContainsString = requires { requires(std::is_same_v<std::string, Types> || ...); };

template <class Type>
concept IsString = std::is_same_v<std::remove_cvref_t<Type>, std::string>;

class DynamicField
{
public:
    explicit DynamicField(std::span<const uint8_t> address, DataType physicalType);

    template <class Type>
    requires IsNesType<Type> && std::is_pointer<Type>::value
    [[nodiscard]] Type read() const
    {
        if (not physicalType.isSameDataType<Type>())
        {
            throw CannotAccessBuffer("Wrong field type passed. Field is of type {} but accessed as {}", physicalType, typeid(Type).name());
        }
        return reinterpret_cast<Type>(const_cast<uint8_t*>(address.data()));
    };

    template <class Type>
    requires(IsNesType<Type> && not std::is_pointer<Type>::value)
    [[nodiscard]] Type& read() const
    {
        if (not physicalType.isSameDataType<Type>())
        {
            throw CannotAccessBuffer("Wrong field type passed. Field is of type {} but accessed as {}", physicalType, typeid(Type).name());
        }
        return *reinterpret_cast<Type*>(const_cast<uint8_t*>(address.data()));
    };

    template <class Type>
    requires(NESIdentifier<Type> && not std::is_pointer<Type>::value)
    inline Type read() const
    {
        if (not physicalType.isSameDataType<typename Type::Underlying>())
        {
            throw CannotAccessBuffer("Wrong field type passed. Field is of type {} but accessed as {}", physicalType, typeid(Type).name());
        }
        return Type(*reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address.data())));
    };

    template <class Type>
    requires(IsNesType<Type>)
    void write(Type value)
    {
        if (not physicalType.isSameDataType<Type>())
        {
            throw CannotAccessBuffer("Wrong field type passed. Field is of type {} but accessed as {}", physicalType, typeid(Type).name());
        }
        *reinterpret_cast<Type*>(const_cast<uint8_t*>(address.data())) = value;
    };

    template <class Type>
    requires(NESIdentifier<Type>)
    void write(Type value)
    {
        if (not physicalType.isSameDataType<typename Type::Underlying>())
        {
            throw CannotAccessBuffer("Wrong field type passed. Field is of type {} but accessed as {}", physicalType, typeid(Type).name());
        }
        *reinterpret_cast<typename Type::Underlying*>(const_cast<uint8_t*>(address.data())) = value.getRawValue();
    };

    [[nodiscard]] std::string toString() const;

    [[nodiscard]] bool equal(const DynamicField& rhs) const;

    bool operator==(const DynamicField& rhs) const;

    bool operator!=(const DynamicField& rhs) const;

    [[nodiscard]] const DataType& getPhysicalType() const;

    [[nodiscard]] std::span<const uint8_t> getAddressPointer() const;

private:
    std::span<const uint8_t> address;
    DataType physicalType;
};

class DynamicTuple
{
public:
    DynamicTuple(uint64_t tupleIndex, std::shared_ptr<MemoryLayout> memoryLayout, TupleBuffer buffer);

    DynamicField operator[](std::size_t fieldIndex) const;


    DynamicField operator[](std::string fieldName) const;

    void writeVarSized(std::variant<const uint64_t, const std::string> field, std::string value, AbstractBufferProvider& bufferProvider);

    std::string readVarSized(std::variant<const uint64_t, const std::string> field);

    [[nodiscard]] std::string toString(const Schema& schema) const;

    bool operator==(const DynamicTuple& other) const;
    bool operator!=(const DynamicTuple& other) const;

private:
    uint64_t tupleIndex;
    std::shared_ptr<MemoryLayout> memoryLayout;
    TupleBuffer buffer;
};

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
    explicit TestTupleBuffer(const std::shared_ptr<MemoryLayout>& memoryLayout, const TupleBuffer& buffer);

    static TestTupleBuffer createTestTupleBuffer(const TupleBuffer& buffer, const Schema& schema);

    [[nodiscard]] uint64_t getCapacity() const;

    [[nodiscard]] uint64_t getNumberOfTuples() const;

    void setNumberOfTuples(uint64_t value);


    DynamicTuple operator[](std::size_t tupleIndex) const;

    TupleBuffer getBuffer();

    class TupleIterator : public std::iterator<
                              std::input_iterator_tag,
                              DynamicTuple,
                              DynamicTuple,
                              DynamicTuple*,
                              DynamicTuple
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

    TupleIterator begin() const;

    TupleIterator end() const;

    friend std::ostream& operator<<(std::ostream& os, const TestTupleBuffer& buffer);

    [[nodiscard]] std::string toString(const Schema& schema) const;
    [[nodiscard]] std::string toString(const Schema& schema, PrintMode printMode) const;

    template <typename... Types>
    requires(!ContainsString<Types> && ...)
    void pushRecordToBuffer(std::tuple<Types...> record)
    {
        pushRecordToBufferAtIndex(record, buffer.getNumberOfTuples());
    }

    template <typename... Types>
    requires(ContainsString<Types> || ...)
    void pushRecordToBuffer(std::tuple<Types...> record, BufferManager* bufferManager)
    {
        pushRecordToBufferAtIndex(record, buffer.getNumberOfTuples(), bufferManager);
    }

    template <typename... Types>
    void pushRecordToBufferAtIndex(std::tuple<Types...> record, uint64_t recordIndex, AbstractBufferProvider* bufferProvider = nullptr)
    {
        uint64_t numberOfRecords = buffer.getNumberOfTuples();
        uint64_t fieldIndex = 0;
        if (recordIndex >= buffer.getBufferSize())
        {
            throw CannotAccessBuffer(
                "Current buffer is not big enough for index. Current buffer size: " + std::to_string(buffer.getBufferSize())
                + ", Index: " + std::to_string(recordIndex));
        }
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

    template <typename... Types>
    std::tuple<Types...> readRecordFromBuffer(uint64_t recordIndex)
    {
        PRECONDITION(
            (sizeof...(Types)) == memoryLayout->getSchema().getNumberOfFields(),
            "Provided tuple types: {} do not match the number of fields in the memory layout: {}",
            sizeof...(Types),
            memoryLayout->getSchema().getNumberOfFields());
        std::tuple<Types...> retTuple;
        copyRecordFromBufferToTuple(retTuple, recordIndex);
        return retTuple;
    }

    uint64_t countOccurrences(DynamicTuple& tuple) const;

    [[nodiscard]] const MemoryLayout& getMemoryLayout() const;

private:
    template <size_t I = 0, typename... Types>
    void copyRecordFromBufferToTuple(std::tuple<Types...>& record, uint64_t recordIndex)
    {
        if constexpr (I != sizeof...(Types))
        {
            if constexpr (IsString<typename std::tuple_element<I, std::tuple<Types...>>::type>)
            {
                auto childBufferIdx = (*this)[recordIndex][I].read<TupleBuffer::NestedTupleBufferKey>();
                std::get<I>(record) = readVarSizedData(this->buffer, childBufferIdx);
            }
            else
            {
                std::get<I>(record) = ((*this)[recordIndex][I]).read<typename std::tuple_element<I, std::tuple<Types...>>::type>();
            }

            copyRecordFromBufferToTuple<I + 1>(record, recordIndex);
        }
    }

    std::shared_ptr<MemoryLayout> memoryLayout;
    TupleBuffer buffer;
};

}
