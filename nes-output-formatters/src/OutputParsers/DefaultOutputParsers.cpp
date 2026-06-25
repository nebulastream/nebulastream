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

#include <OutputParsers/DefaultOutputParsers.hpp>

#include <charconv>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/InlineTagMacro.hpp>
#include <Util/InvokeMacro.hpp>
#include <nautilus/inline.hpp>
#include <OutputParserRegistry.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// Format `value` as fixed 6-decimal with trailing zeros stripped (keeping one digit after the point) --
/// byte-identical to Util::formatFloat (`fmt::format("{:.6f}")` + strip) -- but via std::to_chars straight
/// into the caller's stack buffer: no per-row format-string parse and no std::string heap allocation (these
/// dominated float-output cost in profiling). Returns the number of chars written into `out`.
/// `cap` must cover the widest fixed-6 form (~316 chars for a 1e308 double).
template <std::floating_point T>
size_t formatFloatFixed6(const T value, char* const out, const size_t cap)
{
    const auto [ptr, ec] = std::to_chars(out, out + cap, value, std::chars_format::fixed, 6);
    const size_t len = static_cast<size_t>(ptr - out);
    const size_t decimalPos = std::string_view{out, len}.find('.');
    if (decimalPos == std::string_view::npos)
    {
        return len;
    }
    size_t lastNonZero = len - 1;
    while (out[lastNonZero] == '0')
    {
        --lastNonZero;
    }
    return (lastNonZero == decimalPos) ? (decimalPos + 2) : (lastNonZero + 1);
}

/// Serialize an integral `value` as decimal ASCII via std::to_chars -- byte-identical to the prior
/// std::to_string path (both emit canonical decimal) but with no heap std::string allocation and no trailing
/// strlen (std::to_chars returns the end pointer). The widest decimal form across our integer types is 20
/// chars (uint64 max / int64 min). When that fits in the remaining main-buffer space, serialize STRAIGHT into
/// the output buffer -- skipping the stack buffer and the writeBytesToBuffer memcpy (mirrors the ZMIJ float
/// direct-write fast path). Only at a buffer boundary do we serialize into a stack buffer and let
/// writeBytesToBuffer spill the overflow into a child buffer.
template <std::integral T>
uint64_t writeIntegralValue(
    const T value,
    int8_t* const bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* const tupleBuffer,
    AbstractBufferProvider* const bufferProvider)
{
    constexpr uint64_t maxWidth = 20;
    if (remainingSpace >= maxWidth)
    {
        char* const out = reinterpret_cast<char*>(bufferStartingAddress);
        const auto [ptr, ec] = std::to_chars(out, out + remainingSpace, value);
        return static_cast<uint64_t>(ptr - out);
    }
    char tmp[24];
    const auto [ptr, ec] = std::to_chars(tmp, tmp + sizeof(tmp), value);
    return writeBytesToBuffer(tmp, static_cast<size_t>(ptr - tmp), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Direct-write counterpart for floats (the same lever as writeIntegralValue): when the widest fixed-6 form
/// fits in the remaining main-buffer space, formatFloatFixed6 serializes STRAIGHT into the output buffer --
/// skipping the stack buffer and the writeBytesToBuffer memcpy. The widest fixed-6 form is ~47 chars for a
/// float (3.4e38) and ~317 for a double (1.7e308); 48 / 344 bound those with slack. formatFloatFixed6 writes
/// the full fixed-6 form (with trailing zeros) but returns the shorter stripped length; in direct-write mode
/// the stripped-off zero bytes sit beyond the field's logical length and are overwritten by the next field
/// write -- harmless, since used-size accounting is by the returned length. At a buffer boundary we serialize
/// into a stack buffer and let writeBytesToBuffer spill the overflow into a child buffer.
template <std::floating_point T>
uint64_t writeFloatFixed6Value(
    const T value,
    int8_t* const bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* const tupleBuffer,
    AbstractBufferProvider* const bufferProvider)
{
    constexpr uint64_t maxWidth = sizeof(T) == sizeof(float) ? 48 : 344;
    if (remainingSpace >= maxWidth)
    {
        return formatFloatFixed6(value, reinterpret_cast<char*>(bufferStartingAddress), remainingSpace);
    }
    char tmp[344];
    const size_t n = formatFloatFixed6(value, tmp, sizeof(tmp));
    return writeBytesToBuffer(tmp, n, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseChar(
    const char value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeBytesToBuffer(&value, 1, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF32(
    const float value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeFloatFixed6Value(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseF64(
    const double value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeFloatFixed6Value(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseInt8(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseInt16(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseInt32(
    const int32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseInt64(
    const int64_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseBool(
    const bool value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(static_cast<int>(value), bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseUint8(
    const uint8_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseUint16(
    const uint16_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseUint32(
    const uint32_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

NAUTILUS_TAGGED_INLINE(output_parse)

uint64_t parseUint64(
    const uint64_t value,
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeIntegralValue(value, bufferStartingAddress, remainingSpace, tupleBuffer, bufferProvider);
}

nautilus::val<uint64_t> DefaultCHAROutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<char>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseChar, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<float>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultF64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<double>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseF64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<int64_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseInt64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultBOOLOutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<bool>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseBool, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT8OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint8_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint8, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT16OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint16_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint16, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT32OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint32_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint32, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

nautilus::val<uint64_t> DefaultUINT64OutputParser::parseAndWrite(
    const VarVal& value,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<int8_t*>& startingAddress) const
{
    const auto castedVal = value.getRawValueAs<nautilus::val<uint64_t>>();
    return NAUTILUS_TAGGED_INVOKE(
        "parse_to_string", parseUint64, castedVal, startingAddress, remainingSize, recordBuffer.getReference(), bufferProvider);
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultCHAROutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultCHAROutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultF32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultF32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultF64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultF64OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT8OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT8OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT16OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT16OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultINT64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultINT64OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultBOOLOutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultBOOLOutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT8OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT8OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT16OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT16OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT32OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT32OutputParser>();
}

OutputParserRegistryReturnType OutputParserGeneratedRegistrar::RegisterDefaultUINT64OutputParser(OutputParserRegistryArguments)
{
    return std::make_unique<DefaultUINT64OutputParser>();
}
}
