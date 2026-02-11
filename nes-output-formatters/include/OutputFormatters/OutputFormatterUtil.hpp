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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Allocate child buffers to write the string completely into the tuple buffer.
/// String may span between children or between the main buffer and the first child.
/// RemainingSpace tells the function the amount of space that is left in the main buffer.
inline void writeWithChildBuffers(
    const std::string& value,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    size_t remainingBytes = value.size();
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        std::memcpy(bufferStartingAddress, value.c_str(), remainingSpace);
        remainingBytes -= remainingSpace;
        /// Create the first child buffer, if necessary
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            numOfChildBuffers++;
        }
    }
    while (remainingBytes > 0)
    {
        /// Write as many bytes in the latest child buffer as possible and allocate a new one if space does not suffice
        const VariableSizedAccess::Index childIndex(numOfChildBuffers - 1);
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = value.size() - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, value.c_str() + valueOffset, writable);
        remainingBytes -= writable;
        lastChildBuffer.setNumberOfTuples(bufferOffset + writable);
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            numOfChildBuffers++;
        }
    }
}

template <typename T>
static uint64_t writeValAsString(
    const T val,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const DataType* physicalType,
    const bool allowChildren,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Convert val to a string
    /// In the future, users could provide custom conversions for a specific datatype T in order to optimize.
    const std::string stringFormattedValue(physicalType->formattedBytesToString(&val));

    /// Write string into the memory at starting address. If there is not enough space, allocate children, if allowed.
    const size_t stringSize = stringFormattedValue.size();
    if (stringSize > remainingSpace)
    {
        if (allowChildren)
        {
            writeWithChildBuffers(stringFormattedValue, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
            return remainingSpace;
        }
        return std::numeric_limits<uint64_t>::max();
    }
    std::memcpy(bufferStartingAddress, stringFormattedValue.c_str(), stringSize);
    return stringSize;
}

/// Converts the varval value of the given physical type into a string representation.
/// The string is then written into the memory of the record buffer, starting from address.
/// Should the space not suffice and allowChildren be set to true, it uses writeWithChildrenBuffers to allocate new space.
/// Returns the amount of bytes written in the record buffer itself, children excluded.
inline nautilus::val<uint64_t> formatAndWriteVal(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::val<int8_t*>& address,
    const nautilus::val<uint64_t>& remainingSize,
    const nautilus::val<bool>& allowChildren,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// Switch between datatypes to convert value to a nautilus val of the phyiscal type and convert it to a string
    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN: {
            const auto castedVal = value.cast<nautilus::val<bool>>();
            return nautilus::invoke(
                writeValAsString<bool>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::INT8: {
            const auto castedVal = value.cast<nautilus::val<int8_t>>();
            return nautilus::invoke(
                writeValAsString<int8_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::INT16: {
            const auto castedVal = value.cast<nautilus::val<int16_t>>();
            return nautilus::invoke(
                writeValAsString<int16_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::INT32: {
            const auto castedVal = value.cast<nautilus::val<int32_t>>();
            return nautilus::invoke(
                writeValAsString<int32_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::INT64: {
            const auto castedVal = value.cast<nautilus::val<int64_t>>();
            return nautilus::invoke(
                writeValAsString<int64_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::CHAR: {
            const auto castedVal = value.cast<nautilus::val<char>>();
            return nautilus::invoke(
                writeValAsString<char>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::UINT8: {
            const auto castedVal = value.cast<nautilus::val<uint8_t>>();
            return nautilus::invoke(
                writeValAsString<uint8_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::UINT16: {
            const auto castedVal = value.cast<nautilus::val<uint16_t>>();
            return nautilus::invoke(
                writeValAsString<uint16_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::UINT32: {
            const auto castedVal = value.cast<nautilus::val<uint32_t>>();
            return nautilus::invoke(
                writeValAsString<uint32_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::UINT64: {
            const auto castedVal = value.cast<nautilus::val<uint64_t>>();
            return nautilus::invoke(
                writeValAsString<uint64_t>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::FLOAT32: {
            const auto castedVal = value.cast<nautilus::val<float>>();
            return nautilus::invoke(
                writeValAsString<float>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        case DataType::Type::FLOAT64: {
            const auto castedVal = value.cast<nautilus::val<double>>();
            return nautilus::invoke(
                writeValAsString<double>,
                castedVal,
                address,
                remainingSize,
                nautilus::val<const DataType*>(&fieldType),
                allowChildren,
                recordBuffer.getReference(),
                bufferProvider);
        }
        default: {
            return std::numeric_limits<uint64_t>::max();
        }
    }
}
}
