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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <include/Runtime/TupleBuffer.hpp>
#include <include/Util/TestTupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Memory::MemoryLayouts
{

DynamicField::DynamicField(const uint8_t* address, DataType physicalType) : address(address), physicalType(std::move(physicalType))
{
}

DynamicField DynamicTuple::operator[](const std::size_t fieldIndex) const
{
    auto* bufferBasePointer = buffer.getBuffer<uint8_t>();
    const auto offset = memoryLayout->getFieldOffset(tupleIndex, fieldIndex);
    auto* basePointer = bufferBasePointer + offset;
    return DynamicField{basePointer, memoryLayout->getPhysicalType(fieldIndex)};
}

DynamicField DynamicTuple::operator[](std::string fieldName) const
{
    const auto fieldIndex = memoryLayout->getFieldIndexFromName(fieldName);
    if (!fieldIndex.has_value())
    {
        throw CannotAccessBuffer("field name {} does not exist in layout", fieldName);
    }
    return this->operator[](memoryLayout->getFieldIndexFromName(fieldName).value());
}

DynamicTuple::DynamicTuple(const uint64_t tupleIndex, std::shared_ptr<MemoryLayout> memoryLayout, Memory::TupleBuffer buffer)
    : tupleIndex(tupleIndex), memoryLayout(std::move(memoryLayout)), buffer(std::move(buffer))
{
}

void DynamicTuple::writeVarSized(
    std::variant<const uint64_t, const std::string> field, std::string value, Memory::AbstractBufferProvider& bufferProvider)
{
    const auto valueLength = value.length();
    auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
    if (childBuffer.has_value())
    {
        auto& childBufferVal = childBuffer.value();
        *childBufferVal.getBuffer<uint32_t>() = valueLength;
        std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), value.c_str(), valueLength);
        auto index = buffer.storeChildBuffer(childBufferVal);
        std::visit(
            [this, index](const auto& key)
            {
                if constexpr (
                    std::is_convertible_v<std::decay_t<decltype(key)>, std::size_t>
                    || std::is_convertible_v<std::decay_t<decltype(key)>, std::string>)
                {
                    (*this)[key].write(index);
                }
                else
                {
                    PRECONDITION(
                        false, "We expect either a uint64_t or a std::string to access a DynamicField, but got: {}", typeid(key).name());
                }
            },
            field);
    }
    else
    {
        NES_ERROR("Could not store string {}", value);
    }
}

std::string DynamicTuple::readVarSized(std::variant<const uint64_t, const std::string> field)
{
    return std::visit(
        [this](const auto& key)
        {
            if constexpr (
                std::is_convertible_v<std::decay_t<decltype(key)>, std::size_t>
                || std::is_convertible_v<std::decay_t<decltype(key)>, std::string>)
            {
                auto index = (*this)[key].template read<Memory::TupleBuffer::NestedTupleBufferKey>();
                return readVarSizedData(this->buffer, index);
            }
            else
            {
                PRECONDITION(
                    false, "We expect either a uint64_t or a std::string to access a DynamicField, but got: {}", typeid(key).name());
            }
        },
        field);
}

std::string DynamicTuple::toString(const Schema& schema) const
{
    std::stringstream ss;
    for (uint32_t i = 0; i < schema.getNumberOfFields(); ++i)
    {
        const auto* const fieldEnding = (i < schema.getNumberOfFields() - 1) ? "|" : "";
        const auto dataType = schema.getFieldAt(i).dataType;
        DynamicField currentField = this->operator[](i);
        if (dataType.isType(DataType::Type::VARSIZED))
        {
            const auto index = currentField.read<Memory::TupleBuffer::NestedTupleBufferKey>();
            const auto string = readVarSizedData(buffer, index);
            ss << string << fieldEnding;
        }
        else if (dataType.isFloat())
        {
            const auto formattedFloatValue = (dataType.getSizeInBytes() == 8) ? Util::formatFloat(currentField.read<double>())
                                                                              : Util::formatFloat(currentField.read<float>());
            ss << formattedFloatValue << fieldEnding;
        }
        else
        {
            ss << currentField.toString() << fieldEnding;
        }
    }
    return ss.str();
}

bool DynamicTuple::operator!=(const DynamicTuple& other) const
{
    return !(*this == other);
}

bool DynamicTuple::operator==(const DynamicTuple& other) const
{
    if (this->memoryLayout->getSchema() != other.memoryLayout->getSchema())
    {
        NES_DEBUG("Schema is not the same! Therefore the tuple can not be the same!");
        return false;
    }

    return std::ranges::all_of(
        this->memoryLayout->getSchema().getFields(),
        [this, &other](const auto& field)
        {
            if (!other.memoryLayout->getSchema().getFieldByName(field.name))
            {
                NES_ERROR("Field with name {} is not contained in both tuples!", field.name);
                return false;
            }

            auto thisDynamicField = (*this)[field.name];
            auto otherDynamicField = other[field.name];

            if (field.dataType.isType(DataType::Type::VARSIZED))
            {
                const auto thisString = readVarSizedData(buffer, thisDynamicField.template read<TupleBuffer::NestedTupleBufferKey>());
                const auto otherString
                    = readVarSizedData(other.buffer, otherDynamicField.template read<TupleBuffer::NestedTupleBufferKey>());
                return thisString == otherString;
            }
            return thisDynamicField == otherDynamicField;
        });
}

std::string DynamicField::toString() const
{
    return this->physicalType.formattedBytesToString(this->address);
}

bool DynamicField::operator==(const DynamicField& rhs) const
{
    PRECONDITION(physicalType == rhs.physicalType, "Physical types have to be the same but are {} and {}", physicalType, rhs.physicalType);

    return std::memcmp(address, rhs.address, physicalType.getSizeInBytes()) == 0;
};

bool DynamicField::operator!=(const DynamicField& rhs) const
{
    return not(*this == rhs);
}

const DataType& DynamicField::getPhysicalType() const
{
    return physicalType;
}

const uint8_t* DynamicField::getAddressPointer() const
{
    return address;
}

uint64_t TestTupleBuffer::getCapacity() const
{
    return memoryLayout->getCapacity();
}

uint64_t TestTupleBuffer::getNumberOfTuples() const
{
    return buffer.getNumberOfTuples();
}

void TestTupleBuffer::setNumberOfTuples(const uint64_t value)
{
    buffer.setNumberOfTuples(value);
}

DynamicTuple TestTupleBuffer::operator[](std::size_t tupleIndex) const
{
    if (tupleIndex >= getCapacity())
    {
        throw CannotAccessBuffer("index {} is out of bound for capacity {}", std::to_string(tupleIndex), std::to_string(getCapacity()));
    }
    return {tupleIndex, memoryLayout, buffer};
}

TestTupleBuffer::TestTupleBuffer(const std::shared_ptr<MemoryLayout>& memoryLayout, const Memory::TupleBuffer& buffer)
    : memoryLayout(memoryLayout), buffer(buffer)
{
    PRECONDITION(
        memoryLayout->getBufferSize() == buffer.getBufferSize(),
        "Buffer size must be the same compared to the size specified in the layout: {}, but was: {}",
        memoryLayout->getBufferSize(),
        buffer.getBufferSize());
}

Memory::TupleBuffer TestTupleBuffer::getBuffer()
{
    return buffer;
}
std::ostream& operator<<(std::ostream& os, const TestTupleBuffer& buffer)
{
    const auto str = buffer.toString(buffer.memoryLayout->getSchema());
    os << str;
    return os;
}

TestTupleBuffer::TupleIterator TestTupleBuffer::begin() const
{
    return TupleIterator(*this);
}
TestTupleBuffer::TupleIterator TestTupleBuffer::end() const
{
    return TupleIterator(*this, getNumberOfTuples());
}

std::string TestTupleBuffer::toString(const Schema& schema) const
{
    return toString(schema, PrintMode::SHOW_HEADER_END_IN_NEWLINE);
}

std::string TestTupleBuffer::toString(const Schema& schema, const PrintMode printMode) const
{
    std::stringstream str;
    std::vector<uint32_t> physicalSizes;
    std::vector<DataType> types;
    for (const auto& field : schema.getFields())
    {
        auto physicalType = field.dataType;
        physicalSizes.push_back(physicalType.getSizeInBytes());
        types.push_back(physicalType);
        NES_TRACE(
            "TestTupleBuffer: {} {} {} {}",
            std::string("Field Size "),
            field,
            std::string(": "),
            std::to_string(physicalType.getSizeInBytes()));
    }

    if (printMode == PrintMode::SHOW_HEADER_END_IN_NEWLINE or printMode == PrintMode::SHOW_HEADER_END_WITHOUT_NEWLINE)
    {
        str << "+----------------------------------------------------+" << std::endl;
        str << "|";
        for (const auto& field : schema.getFields())
        {
            str << field.name << ":" << magic_enum::enum_name(field.dataType.type) << "|";
        }
        str << std::endl;
        str << "+----------------------------------------------------+" << std::endl;
    }

    auto tupleIterator = this->begin();
    str << (*tupleIterator).toString(schema);
    ++tupleIterator;
    while (tupleIterator != this->end())
    {
        str << std::endl;
        const DynamicTuple dynamicTuple = (*tupleIterator);
        str << dynamicTuple.toString(schema);
        ++tupleIterator;
    }
    if (printMode == PrintMode::SHOW_HEADER_END_IN_NEWLINE or printMode == PrintMode::NO_HEADER_END_IN_NEWLINE)
    {
        str << std::endl;
    }
    if (printMode == PrintMode::SHOW_HEADER_END_IN_NEWLINE or printMode == PrintMode::SHOW_HEADER_END_WITHOUT_NEWLINE)
    {
        str << "+----------------------------------------------------+";
    }
    return str.str();
}

TestTupleBuffer::TupleIterator::TupleIterator(const TestTupleBuffer& buffer) : TupleIterator(buffer, 0)
{
}

TestTupleBuffer::TupleIterator::TupleIterator(const TestTupleBuffer& buffer, const uint64_t currentIndex)
    : buffer(buffer), currentIndex(currentIndex)
{
}

TestTupleBuffer::TupleIterator& TestTupleBuffer::TupleIterator::operator++()
{
    currentIndex++;
    return *this;
}

TestTupleBuffer::TupleIterator::TupleIterator(const TupleIterator& other) : TupleIterator(other.buffer, other.currentIndex)
{
}

const TestTupleBuffer::TupleIterator TestTupleBuffer::TupleIterator::operator++(int)
{
    TupleIterator retval = *this;
    ++(*this);
    return retval;
}

bool TestTupleBuffer::TupleIterator::operator==(TupleIterator other) const
{
    return currentIndex == other.currentIndex && &buffer == &other.buffer;
}

bool TestTupleBuffer::TupleIterator::operator!=(TupleIterator other) const
{
    return !(*this == other);
}

DynamicTuple TestTupleBuffer::TupleIterator::operator*() const
{
    return buffer[currentIndex];
}

const MemoryLayout& TestTupleBuffer::getMemoryLayout() const
{
    return *memoryLayout;
}

TestTupleBuffer TestTupleBuffer::createTestTupleBuffer(const Memory::TupleBuffer& buffer, const Schema& schema)
{
    if (schema.memoryLayoutType == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        auto memoryLayout = std::make_shared<RowLayout>(buffer.getBufferSize(), schema);
        return TestTupleBuffer(std::move(memoryLayout), buffer);
    }
    if (schema.memoryLayoutType == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        auto memoryLayout = std::make_shared<ColumnLayout>(buffer.getBufferSize(), schema);
        return TestTupleBuffer(std::move(memoryLayout), buffer);
    }
    else
    {
        throw NotImplemented("Schema MemoryLayoutType not supported", magic_enum::enum_name(schema.memoryLayoutType));
    }
}

uint64_t TestTupleBuffer::countOccurrences(DynamicTuple& tuple) const
{
    uint64_t count = 0;
    for (auto&& currentTupleIt : *this)
    {
        if (currentTupleIt == tuple)
        {
            ++count;
        }
    }

    return count;
}

}
