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
#include <string>
#include <string_view>
#include <type_traits>
#include <zmij.h>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Write the string completely into the tuple buffer.
/// Child buffers may be allocated if it does not fit completely into the main memory of the tuple buffer.
/// String may span between children or between the main buffer and the first child.
/// RemainingSpace tells the function the amount of space that is left in the main buffer.
/// Will return the amount of bytes written in the main memory of the buffer
inline uint64_t writeValueToBuffer(
    const char* value,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider,
    int8_t* bufferStartingAddress)
{
    const std::string_view valueString{value};
    size_t remainingBytes = valueString.size();
    uint32_t numOfChildBuffers = tupleBuffer->getNumberOfChildBuffers();
    uint64_t writtenToMainMemory = 0;
    /// Fill up the remaing space in the main tuple buffer before allocating any child buffers
    if (numOfChildBuffers == 0)
    {
        const size_t fitsInMainBuffer = std::min(remainingBytes, remainingSpace);
        writtenToMainMemory += fitsInMainBuffer;
        std::memcpy(bufferStartingAddress, valueString.data(), fitsInMainBuffer);
        remainingBytes -= fitsInMainBuffer;
        /// Create the first child buffer, if necessary
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    while (remainingBytes > 0)
    {
        /// Write as many bytes in the latest child buffer as possible and allocate a new one if space does not suffice
        const VariableSizedAccess::Index childIndex{numOfChildBuffers - 1};
        auto lastChildBuffer = tupleBuffer->loadChildBuffer(childIndex);
        const auto bufferOffset = lastChildBuffer.getNumberOfTuples();
        const uint32_t valueOffset = valueString.size() - remainingBytes;
        const uint64_t writable = std::min(remainingBytes, lastChildBuffer.getBufferSize() - bufferOffset);
        std::memcpy(lastChildBuffer.getAvailableMemoryArea<>().data() + bufferOffset, valueString.data() + valueOffset, writable);
        remainingBytes -= writable;
        lastChildBuffer.setNumberOfTuples(bufferOffset + writable);
        if (remainingBytes > 0)
        {
            auto newChildBuffer = bufferProvider->getBufferBlocking();
            (void)tupleBuffer->storeChildBuffer(newChildBuffer);
            ++numOfChildBuffers;
        }
    }
    return writtenToMainMemory;
}

template <typename T>
static uint64_t writeValAsString(
    const T val,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    /// Convert val to a string
    /// Depending on the type, we need to perform additional transformations besides the direct conversion to string
    /// In the future, we could introduce customizable parsing functions for every data type via a registry
    using removedCVRefT = std::remove_cvref_t<T>;
    std::string stringFormattedValue;
    if constexpr (std::is_same_v<removedCVRefT, float> || std::is_same_v<removedCVRefT, double>)
    {
        stringFormattedValue = std::string(zmij::double_buffer_size + 1, '\0');
        auto size = zmij::write(stringFormattedValue.data(), stringFormattedValue.size(), val);
        stringFormattedValue.resize(size);
        // stringFormattedValue = formatFloat(val);
    }
    else if constexpr (std::is_same_v<removedCVRefT, bool>)
    {
        stringFormattedValue = std::to_string(static_cast<int>(val));
    }
    else if constexpr (std::is_same_v<removedCVRefT, char>)
    {
        stringFormattedValue = std::string{val};
    }
    else
    {
        stringFormattedValue = std::to_string(val);
    }

    /// Write string into the memory at starting address
    return writeValueToBuffer(stringFormattedValue.c_str(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Converts the varval value of the given physical type into a string representation.
/// The string is then written into the memory of the record buffer, starting from address.
/// Child buffers might be allocated to fit the whole string into the buffer.
/// Returns the amount of bytes written in the record buffer itself, children excluded.
inline nautilus::val<uint64_t> formatAndWriteVal(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::val<int8_t*>& address,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    nautilus::val<uint64_t> writtenBytes = 0;
    /// Switch between datatypes to convert value to a nautilus val of the phyiscal type and convert it to a string
    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN: {
            const auto castedVal = value.getRawValueAs<nautilus::val<bool>>();
            writtenBytes
                = nautilus::invoke(writeValAsString<bool>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::INT8: {
            /// For some reason, casting INT8 and INT16 values to their respected c++ types, leads to the to_string call in writeVal treating them as unsigned
            /// Casting them to int32_t fixes this
            const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<int32_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::INT16: {
            const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<int32_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::INT32: {
            const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<int32_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::INT64: {
            const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<int64_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::CHAR: {
            const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
            writtenBytes
                = nautilus::invoke(writeValAsString<char>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::UINT8: {
            const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<uint8_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::UINT16: {
            const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<uint16_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::UINT32: {
            const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<uint32_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::UINT64: {
            const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<uint64_t>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::FLOAT32: {
            const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
            writtenBytes
                = nautilus::invoke(writeValAsString<float>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::FLOAT64: {
            const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
            writtenBytes = nautilus::invoke(
                writeValAsString<double>, castedVal, address, remainingSize, recordBuffer.getReference(), bufferProvider);
            break;
        }
        case DataType::Type::VARSIZED:
        case DataType::Type::UNDEFINED: {
            INVARIANT(false, "formatAndWriteVal is not supported for VARSIZED and UNDEFINED types.");
        }
    }
    return writtenBytes;
}
}
