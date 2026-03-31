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

#include <UncompiledRawValueParser.hpp>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <DataTypes/DataType.hpp>
// #include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void writeVarsizedToChildBuffer(
    const std::string_view inputString,
    const size_t writeOffsetInBytes,
    const size_t childBufferIdx,
    TupleBuffer& tupleBufferFormatted,
    TupleBuffer& childBuffer)
{
    std::memcpy(childBuffer.getAvailableMemoryArea().data() + childBuffer.getNumberOfTuples(), inputString.data(), inputString.size());
    *reinterpret_cast<VariableSizedAccess*>(tupleBufferFormatted.getAvailableMemoryArea().data() + writeOffsetInBytes)
        = VariableSizedAccess{
            static_cast<VariableSizedAccess::Index>(childBufferIdx),
            static_cast<VariableSizedAccess::Offset>(childBuffer.getNumberOfTuples()),
            static_cast<VariableSizedAccess::Size>(inputString.size())};
    childBuffer.setNumberOfTuples(childBuffer.getNumberOfTuples() + inputString.size());
}

void writeVarsizedToNewChildBuffer(
    const std::string_view inputString,
    const size_t writeOffsetInBytes,
    TupleBuffer& tupleBufferFormatted,
    AbstractBufferProvider& bufferProvider)
{
    auto newChildBuffer = (tupleBufferFormatted.getBufferSize() >= inputString.size())
        ? bufferProvider.getBufferBlocking()
        : bufferProvider.getUnpooledBuffer(inputString.size());
    if (not newChildBuffer.has_value())
    {
        throw BufferAllocationFailure("could not allocate buffer for varsized of size {}", inputString.size());
    }

    std::memcpy(newChildBuffer.value().getAvailableMemoryArea().data(), inputString.data(), inputString.size());
    newChildBuffer.value().setNumberOfTuples(inputString.size());
    const auto newChildBufferIdx = tupleBufferFormatted.storeChildBuffer(newChildBuffer.value());
    *reinterpret_cast<VariableSizedAccess*>(tupleBufferFormatted.getAvailableMemoryArea().data() + writeOffsetInBytes)
        = VariableSizedAccess{
            static_cast<VariableSizedAccess::Index>(newChildBufferIdx),
            static_cast<VariableSizedAccess::Offset>(0),
            static_cast<VariableSizedAccess::Size>(inputString.size())};
}

// Todo: need arena
UncompiledParseFunctionSignature getQuotedStringParseFunction()
{
    return [](const std::string_view inputString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider& bufferProvider,
              TupleBuffer& tupleBufferFormatted)
    {
        INVARIANT(inputString.length() >= 2, "Input string must be at least 2 characters long.");
        auto inputStringWithoutQuotes = inputString.substr(1, inputString.length() - 2);
        // const Index index, const Offset offset, const Size size
        if (tupleBufferFormatted.getNumberOfChildBuffers() == 0)
        {
            writeVarsizedToNewChildBuffer(inputStringWithoutQuotes, writeOffsetInBytes, tupleBufferFormatted, bufferProvider);
            return;
        }

        const size_t lastChildBufferIdx = tupleBufferFormatted.getNumberOfChildBuffers() - 1;
        auto lastChildBuffer = tupleBufferFormatted.loadChildBuffer(static_cast<VariableSizedAccess::Index>(lastChildBufferIdx));
        if (lastChildBuffer.getBufferSize() - lastChildBuffer.getNumberOfTuples() > inputStringWithoutQuotes.size())
        {
            writeVarsizedToChildBuffer(
                inputStringWithoutQuotes, writeOffsetInBytes, lastChildBufferIdx, tupleBufferFormatted, lastChildBuffer);
            return;
        }
        writeVarsizedToNewChildBuffer(inputStringWithoutQuotes, writeOffsetInBytes, tupleBufferFormatted, bufferProvider);
    };
}

UncompiledParseFunctionSignature getBasicStringParseFunction()
{
    return [](const std::string_view inputString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider& bufferProvider,
              TupleBuffer& tupleBufferFormatted)
    {
        if (tupleBufferFormatted.getNumberOfChildBuffers() == 0)
        {
            writeVarsizedToNewChildBuffer(inputString, writeOffsetInBytes, tupleBufferFormatted, bufferProvider);
            return;
        }

        const size_t lastChildBufferIdx = tupleBufferFormatted.getNumberOfChildBuffers() - 1;
        auto lastChildBuffer = tupleBufferFormatted.loadChildBuffer(static_cast<VariableSizedAccess::Index>(lastChildBufferIdx));
        if (lastChildBuffer.getBufferSize() - lastChildBuffer.getNumberOfTuples() > inputString.size())
        {
            writeVarsizedToChildBuffer(inputString, writeOffsetInBytes, lastChildBufferIdx, tupleBufferFormatted, lastChildBuffer);
            return;
        }
        writeVarsizedToNewChildBuffer(inputString, writeOffsetInBytes, tupleBufferFormatted, bufferProvider);
    };
}

UncompiledParseFunctionSignature getStringParseFunction(const UncompiledQuotationType quotationType)
{
    switch (quotationType)
    {
        case UncompiledQuotationType::NONE: {
            return getBasicStringParseFunction();
        }
        case UncompiledQuotationType::DOUBLE_QUOTE: {
            return getQuotedStringParseFunction();
        }
    }
    std::unreachable();
}

UncompiledParseFunctionSignature getBasicTypeParseFunction(const DataType::Type physicalType, const UncompiledQuotationType quotationType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8: {
            return parseUncompiledFieldString<int8_t>();
        }
        case DataType::Type::INT16: {
            return parseUncompiledFieldString<int16_t>();
        }
        case DataType::Type::INT32: {
            return parseUncompiledFieldString<int32_t>();
        }
        case DataType::Type::INT64: {
            return parseUncompiledFieldString<int64_t>();
        }
        case DataType::Type::UINT8: {
            return parseUncompiledFieldString<uint8_t>();
        }
        case DataType::Type::UINT16: {
            return parseUncompiledFieldString<uint16_t>();
        }
        case DataType::Type::UINT32: {
            return parseUncompiledFieldString<uint32_t>();
        }
        case DataType::Type::UINT64: {
            return parseUncompiledFieldString<uint64_t>();
        }
        case DataType::Type::FLOAT32: {
            return parseUncompiledFieldString<float>();
        }
        case DataType::Type::FLOAT64: {
            return parseUncompiledFieldString<double>();
        }
        case DataType::Type::CHAR: {
            return (quotationType == UncompiledQuotationType::NONE) ? parseUncompiledFieldString<char>()
                                                                    : parseQuotedUncompiledFieldString<char>();
        }
        case DataType::Type::BOOLEAN: {
            return parseUncompiledFieldString<bool>();
        }
        case DataType::Type::VARSIZED: {
            return getBasicStringParseFunction();
        }
        case DataType::Type::UNDEFINED: {
            throw NotImplemented("Cannot parse undefined type.");
        }
    }
    return nullptr;
}

UncompiledParseFunctionSignature getUncompiledParseFunction(const DataType::Type physicalType, const UncompiledQuotationType quotationType)
{
    if (physicalType == DataType::Type::VARSIZED)
    {
        return getStringParseFunction(quotationType);
    }
    return getBasicTypeParseFunction(physicalType, quotationType);
}
}
