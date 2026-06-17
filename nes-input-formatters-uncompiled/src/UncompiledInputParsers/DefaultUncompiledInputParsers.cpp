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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <UncompiledInputFormatters/UncompiledInputParser.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <UncompiledInputParserRegistry.hpp>

namespace NES
{
namespace
{

/// Takes a target type and a value represented as a string. Attempts to parse the string to the C++ target type.
/// @Note throws CannotFormatMalformedStringValue if the parsing fails.
/// @Note given a string like '0751' and an integer value, from_chars creates an integer '751' from it. Also, '0.751' becomes '0'.
template <typename T>
auto parseUncompiledFieldString()
{
    return [](const std::string_view fieldValueString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider&,
              TupleBuffer& tupleBufferFormatted)
    {
        const T parsedValue = from_chars_with_exception<T>(fieldValueString);
        const auto parsedValueBytes = std::as_bytes<const T>(std::span<const T>{&parsedValue, 1});
        std::ranges::copy(parsedValueBytes, tupleBufferFormatted.getAvailableMemoryArea().begin() + writeOffsetInBytes);
    };
}

template <typename T>
auto parseQuotedUncompiledFieldString()
{
    return [](const std::string_view quotedFieldValueString,
              const size_t writeOffsetInBytes,
              AbstractBufferProvider&,
              TupleBuffer& tupleBufferFormatted)
    {
        INVARIANT(quotedFieldValueString.length() >= 2, "Input string must be at least 2 characters long.");
        const auto fieldValueString = quotedFieldValueString.substr(1, quotedFieldValueString.length() - 2);
        const T parsedValue = from_chars_with_exception<T>(fieldValueString);
        const auto parsedValueBytes = std::as_bytes<const T>(std::span<const T>{&parsedValue, 1});
        std::ranges::copy(parsedValueBytes, tupleBufferFormatted.getAvailableMemoryArea().begin() + writeOffsetInBytes);
    };
}

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

} /// anonymous namespace

/// Registration functions for the default uncompiled input parsers

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultBOOLUncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<bool>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultCHARUncompiledInputParser(UncompiledInputParserRegistryArguments args)
{
    return (args.quotationType == UncompiledQuotationType::NONE) ? parseUncompiledFieldString<char>()
                                                                 : parseQuotedUncompiledFieldString<char>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultINT8UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<int8_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultINT16UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<int16_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultINT32UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<int32_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultINT64UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<int64_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultUINT8UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<uint8_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultUINT16UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<uint16_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultUINT32UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<uint32_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultUINT64UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<uint64_t>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultF32UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<float>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultF64UncompiledInputParser(UncompiledInputParserRegistryArguments)
{
    return parseUncompiledFieldString<double>();
}

UncompiledInputParserRegistryReturnType
UncompiledInputParserGeneratedRegistrar::RegisterDefaultVARSIZEDUncompiledInputParser(UncompiledInputParserRegistryArguments args)
{
    return getStringParseFunction(args.quotationType);
}
}
