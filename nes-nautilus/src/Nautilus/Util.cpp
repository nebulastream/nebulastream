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

#include <Nautilus/Util.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/val_std.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
void logProxy(const char* message, const LogLevel logLevel)
{
    /// Calling the logger with the message and the log level
    switch (logLevel)
    {
        case LogLevel::LOG_NONE:
            break;
        case LogLevel::LOG_ERROR:
            NES_ERROR("{}", message);
            break;
        case LogLevel::LOG_WARNING:
            NES_WARNING("{}", message);
            break;
        case LogLevel::LOG_INFO:
            NES_INFO("{}", message);
            break;
        case LogLevel::LOG_DEBUG:
            NES_DEBUG("{}", message);
            break;
        case LogLevel::LOG_TRACE:
            NES_TRACE("{}", message);
            break;
    }
}

VarVal createNautilusMinValue(const DataType::Type physicalType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8:
            return createNautilusConstValue(std::numeric_limits<int8_t>::min(), physicalType);
        case DataType::Type::INT16:
            return createNautilusConstValue(std::numeric_limits<int16_t>::min(), physicalType);
        case DataType::Type::INT32:
            return createNautilusConstValue(std::numeric_limits<int32_t>::min(), physicalType);
        case DataType::Type::INT64:
            return createNautilusConstValue(std::numeric_limits<int64_t>::min(), physicalType);
        case DataType::Type::UINT8:
            return createNautilusConstValue(std::numeric_limits<uint8_t>::min(), physicalType);
        case DataType::Type::UINT16:
            return createNautilusConstValue(std::numeric_limits<uint16_t>::min(), physicalType);
        case DataType::Type::UINT32:
            return createNautilusConstValue(std::numeric_limits<uint32_t>::min(), physicalType);
        case DataType::Type::UINT64:
            return createNautilusConstValue(std::numeric_limits<uint64_t>::min(), physicalType);
        case DataType::Type::FLOAT32:
            return createNautilusConstValue(std::numeric_limits<float>::min(), physicalType);
        case DataType::Type::FLOAT64:
            return createNautilusConstValue(std::numeric_limits<double>::min(), physicalType);
        default: {
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(physicalType));
        }
    }
    throw UnknownDataType("Physical Type: type {} is not a BasicPhysicalType", magic_enum::enum_name(physicalType));
}

VarVal createNautilusMaxValue(const DataType::Type physicalType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8:
            return createNautilusConstValue(std::numeric_limits<int8_t>::max(), physicalType);
        case DataType::Type::INT16:
            return createNautilusConstValue(std::numeric_limits<int16_t>::max(), physicalType);
        case DataType::Type::INT32:
            return createNautilusConstValue(std::numeric_limits<int32_t>::max(), physicalType);
        case DataType::Type::INT64:
            return createNautilusConstValue(std::numeric_limits<int64_t>::max(), physicalType);
        case DataType::Type::UINT8:
            return createNautilusConstValue(std::numeric_limits<uint8_t>::max(), physicalType);
        case DataType::Type::UINT16:
            return createNautilusConstValue(std::numeric_limits<uint16_t>::max(), physicalType);
        case DataType::Type::UINT32:
            return createNautilusConstValue(std::numeric_limits<uint32_t>::max(), physicalType);
        case DataType::Type::UINT64:
            return createNautilusConstValue(std::numeric_limits<uint64_t>::max(), physicalType);
        case DataType::Type::FLOAT32:
            return createNautilusConstValue(std::numeric_limits<float>::max(), physicalType);
        case DataType::Type::FLOAT64:
            return createNautilusConstValue(std::numeric_limits<double>::max(), physicalType);
        default: {
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(physicalType));
        }
    }
}

NautilusBuffer NautilusBuffer::load(nautilus::val<const TupleBuffer*> originalBuffer)
{
    NautilusBuffer newBuffer;
    nautilus::invoke(
        +[](const NES::TupleBuffer* originalBuffer, NES::TupleBuffer* newBuffer) { *newBuffer = *originalBuffer; },
        originalBuffer,
        newBuffer.asArg());
    return newBuffer;
}

nautilus::val<int8_t*> NautilusBuffer::data()
{
    return nautilus::invoke(+[](NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, &buffer);
}

nautilus::val<size_t> NautilusBuffer::getNumberOfRecords() const
{
    return nautilus::invoke(
        +[](const NES::TupleBuffer* buffer)
        {
            INVARIANT(buffer->getAvailableMemoryArea<>().data() != nullptr, "Buffer is invalid for NautilusBuffer:getNumberOfRecords()");
            return buffer->getNumberOfTuples();
        },
        asArg());
}

NautilusBuffer NautilusBuffer::getChild(nautilus::val<size_t> index)
{
    NautilusBuffer child;
    nautilus::invoke(
        +[](NES::TupleBuffer* buffer, NES::TupleBuffer* child, size_t index)
        { *child = buffer->loadChildBuffer(NES::VariableSizedAccess::Index{index}); },
        &buffer,
        &child.buffer,
        index);
    return child;
}

nautilus::val<size_t> NautilusBuffer::storeChild(NautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](NES::TupleBuffer* buffer, NES::TupleBuffer* child)
        { return static_cast<size_t>(buffer->storeChildBuffer(*child).getRawIndex()); },
        &buffer,
        &child.buffer);
}

nautilus::val<bool> NautilusBuffer::isValid() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<>().data() != nullptr; }, asArg());
}

nautilus::val<const NES::TupleBuffer*> NautilusBuffer::asArg() const
{
    return &const_cast<nautilus::val<NES::TupleBuffer>&>(buffer);
}

nautilus::val<NES::TupleBuffer*> NautilusBuffer::asArg()
{
    return &buffer;
}

nautilus::val<bool> NautilusBuffer::operator==(const NautilusBuffer& other) const
{
    return nautilus::invoke(
        +[](const TupleBuffer* buffer, const TupleBuffer* other) -> bool
        { return buffer->getAvailableMemoryArea<int8_t>().data() == other->getAvailableMemoryArea<int8_t>().data(); },
        asArg(),
        other.asArg());
}

NautilusBorrowedBuffer::NautilusBorrowedBuffer(const nautilus::val<NES::TupleBuffer*>& buffer) : buffer(buffer)
{
}

NautilusBorrowedBuffer NautilusBorrowedBuffer::load(nautilus::val<TupleBuffer*> originalBuffer)
{
    return NautilusBorrowedBuffer{originalBuffer};
}

nautilus::val<uint8_t*> NautilusBorrowedBuffer::data()
{
    return nautilus::invoke(+[](NES::TupleBuffer* buffer) { return buffer->getAvailableMemoryArea<int8_t>().data(); }, buffer);
}

nautilus::val<size_t> NautilusBorrowedBuffer::getNumberOfRecords() const
{
    return nautilus::invoke(+[](const NES::TupleBuffer* buffer) { return buffer->getNumberOfTuples(); }, buffer);
}

NautilusBuffer NautilusBorrowedBuffer::getChild(nautilus::val<size_t> index)
{
    NautilusBuffer child;
    nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child, size_t index) { *child = self->loadChildBuffer(VariableSizedAccess::Index{index}); },
        buffer,
        child.asArg(),
        index);
    return child;
}

nautilus::val<size_t> NautilusBorrowedBuffer::storeChild(NautilusBuffer&& child)
{
    return nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child) { return self->storeChildBuffer(*child).getRawIndex(); }, buffer, child.asArg());
}

nautilus::val<size_t> NautilusBorrowedBuffer::storeChild(NautilusBorrowedBuffer&& child)
{
    return nautilus::invoke(
        +[](TupleBuffer* self, TupleBuffer* child) { return self->storeChildBuffer(*child).getRawIndex(); }, buffer, child.get());
}

nautilus::val<NES::TupleBuffer*> NautilusBorrowedBuffer::get() const
{
    return buffer;
}

nautilus::val<bool> NautilusBorrowedBuffer::operator==(const NautilusBorrowedBuffer& other) const
{
    return nautilus::invoke(
        +[](const TupleBuffer* buffer, const TupleBuffer* other) -> bool
        { return buffer->getAvailableMemoryArea<uint8_t>().data() == other->getAvailableMemoryArea<uint8_t>().data(); },
        get(),
        other.get());
}

}
