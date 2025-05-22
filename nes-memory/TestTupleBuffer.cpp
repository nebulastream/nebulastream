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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <MemoryLayout/ColumnLayout.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <include/Runtime/BufferManager.hpp>
#include <include/Runtime/TupleBuffer.hpp>
#include <include/Util/TestTupleBuffer.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Memory::MemoryLayouts
{

DynamicField::DynamicField(const uint8_t* address, std::shared_ptr<PhysicalType> physicalType)
    : address(address), physicalType(std::move(std::move(physicalType)))
{
}

DynamicField DynamicTuple::operator[](const std::size_t fieldIndex) const
{
    auto* bufferBasePointer = buffer.getBuffer<uint8_t>();
    const auto offset = memoryLayout->getFieldOffset(tupleIndex, fieldIndex);
    auto* basePointer = bufferBasePointer + offset;
    const auto physicalType = memoryLayout->getPhysicalType(fieldIndex);
    return DynamicField{basePointer, physicalType};
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

std::string DynamicTuple::toString(const std::shared_ptr<Schema>& schema) const
{
    std::stringstream ss;
    for (uint32_t i = 0; i < schema->getFieldCount(); ++i)
    {
        const auto fieldEnding = (i < schema->getFieldCount() - 1) ? "|" : "";
        const auto dataType = schema->getFieldByIndex(i)->getDataType();
        DynamicField currentField = this->operator[](i);
        if (NES::Util::instanceOf<VariableSizedDataType>(dataType))
        {
            const auto index = currentField.read<Memory::TupleBuffer::NestedTupleBufferKey>();
            const auto string = readVarSizedData(buffer, index);
            ss << string << fieldEnding;
        }
        else if (NES::Util::instanceOf<Float>(dataType))
        {
            const auto numBits = NES::Util::as<Float>(dataType)->getBits();
            const auto formattedFloatValue
                = (numBits == 64) ? Util::formatFloat(currentField.read<double>()) : Util::formatFloat(currentField.read<float>());
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
    if (!(*this->memoryLayout->getSchema() == *other.memoryLayout->getSchema()))
    {
        NES_DEBUG("Schema is not the same! Therefore the tuple can not be the same!");
        return false;
    }
    for (const auto& field : *this->memoryLayout->getSchema())
    {
        if (!other.memoryLayout->getSchema()->getFieldByName(field->getName()))
        {
            NES_ERROR("Field with name {} is not contained in both tuples!", field->getName());
            return false;
        }

        auto thisDynamicField = (*this)[field->getName()];
        auto otherDynamicField = other[field->getName()];

        if (NES::Util::instanceOf<VariableSizedDataType>(field->getDataType()))
        {
            const auto thisString = readVarSizedData(buffer, thisDynamicField.read<Memory::TupleBuffer::NestedTupleBufferKey>());
            const auto otherString = readVarSizedData(other.buffer, otherDynamicField.read<Memory::TupleBuffer::NestedTupleBufferKey>());
            if (thisString != otherString)
            {
                return false;
            }
        }
        else
        {
            if (thisDynamicField != otherDynamicField)
            {
                return false;
            }
        }
    }

    return true;
}

std::string DynamicField::toString() const
{
    return this->physicalType->convertRawToString(this->address);
}

bool DynamicField::operator==(const DynamicField& rhs) const
{
    PRECONDITION(
        *physicalType == *rhs.physicalType,
        "Physical types have to be the same but are {} and {}",
        physicalType->toString(),
        rhs.physicalType->toString());

    return std::memcmp(address, rhs.address, physicalType->size()) == 0;
};

bool DynamicField::operator!=(const DynamicField& rhs) const
{
    return not(*this == rhs);
}

const std::shared_ptr<PhysicalType>& DynamicField::getPhysicalType() const
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

TestTupleBuffer::TestTupleBuffer(const std::shared_ptr<MemoryLayout>& memoryLayout, Memory::TupleBuffer buffer)
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
    auto buf = buffer;
    const auto str = buf.toString(buffer.memoryLayout->getSchema());
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

std::string TestTupleBuffer::toString(const std::shared_ptr<Schema>& schema) const
{
    return toString(schema, PrintMode::SHOW_HEADER_END_IN_NEWLINE);
}

std::string TestTupleBuffer::toString(const std::shared_ptr<Schema>& schema, const PrintMode printMode) const
{
    std::stringstream str;
    std::vector<uint32_t> physicalSizes;
    std::vector<std::shared_ptr<PhysicalType>> types;
    const auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (const auto& field : *schema)
    {
        auto physicalType = physicalDataTypeFactory.getPhysicalType(field->getDataType());
        physicalSizes.push_back(physicalType->size());
        types.push_back(physicalType);
        NES_TRACE(
            "TestTupleBuffer: {} {} {} {}",
            std::string("Field Size "),
            field->toString(),
            std::string(": "),
            std::to_string(physicalType->size()));
    }

    if (printMode == PrintMode::SHOW_HEADER_END_IN_NEWLINE or printMode == PrintMode::SHOW_HEADER_END_WITHOUT_NEWLINE)
    {
        str << "+----------------------------------------------------+" << std::endl;
        str << "|";
        for (const auto& field : *schema)
        {
            str << field->getName() << ":" << physicalDataTypeFactory.getPhysicalType(field->getDataType())->toString() << "|";
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
        auto dynamicTuple = *tupleIterator;
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

std::shared_ptr<MemoryLayout> TestTupleBuffer::getMemoryLayout() const
{
    return memoryLayout;
}

TestTupleBuffer TestTupleBuffer::createTestTupleBuffer(const Memory::TupleBuffer& buffer, const std::shared_ptr<Schema>& schema)
{
    if (schema->getLayoutType() == Schema::MemoryLayoutType::ROW_LAYOUT)
    {
        const auto memoryLayout = RowLayout::create(schema, buffer.getBufferSize());
        return TestTupleBuffer(memoryLayout, buffer);
    }
    else if (schema->getLayoutType() == Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
    {
        const auto memoryLayout = ColumnLayout::create(schema, buffer.getBufferSize());
        return TestTupleBuffer(memoryLayout, buffer);
    }
    else
    {
        throw NotImplemented("Schema MemoryLayoutType not supported", magic_enum::enum_name(schema->getLayoutType()));
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
