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

#include <JSONOutputFormatter.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <fmt/format.h>
#include <std/cstring.h>

#include <Configurations/Descriptor.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <OutputParserRegistry.hpp>
#include <function.hpp>
#include <select.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// Append `character` to `out` with JSON string escaping (RFC 8259: `"`, `\`, and control
/// characters below 0x20 must be escaped; everything else passes through byte-for-byte).
void appendJsonEscaped(std::string& out, const char character)
{
    switch (character)
    {
        case '"':
            out += "\\\"";
            return;
        case '\\':
            out += "\\\\";
            return;
        case '\b':
            out += "\\b";
            return;
        case '\f':
            out += "\\f";
            return;
        case '\n':
            out += "\\n";
            return;
        case '\r':
            out += "\\r";
            return;
        case '\t':
            out += "\\t";
            return;
        default:
            if (static_cast<unsigned char>(character) < 0x20)
            {
                constexpr std::string_view hexDigits = "0123456789abcdef";
                out += "\\u00";
                out += hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU];
                out += hexDigits[static_cast<unsigned char>(character) & 0xFU];
                return;
            }
            out += character;
    }
}

/// Host-side (construction-time) JSON escaping for field names.
std::string jsonEscapeString(const std::string_view raw)
{
    std::string escaped;
    escaped.reserve(raw.size());
    for (const char character : raw)
    {
        appendJsonEscaped(escaped, character);
    }
    return escaped;
}

/// Write the escape sequence for one escapable byte into `out`; returns its length. Callers have
/// already established that `character` needs escaping (`"`, `\`, or a control byte < 0x20).
uint64_t writeEscapeSequence(int8_t* out, const char character)
{
    char second = 0;
    switch (character)
    {
        case '"':
            second = '"';
            break;
        case '\\':
            second = '\\';
            break;
        case '\b':
            second = 'b';
            break;
        case '\f':
            second = 'f';
            break;
        case '\n':
            second = 'n';
            break;
        case '\r':
            second = 'r';
            break;
        case '\t':
            second = 't';
            break;
        default: {
            constexpr std::string_view hexDigits = "0123456789abcdef";
            out[0] = '\\';
            out[1] = 'u';
            out[2] = '0';
            out[3] = '0';
            out[4] = static_cast<int8_t>(hexDigits[(static_cast<unsigned char>(character) >> 4U) & 0xFU]);
            out[5] = static_cast<int8_t>(hexDigits[static_cast<unsigned char>(character) & 0xFU]);
            return 6;
        }
    }
    out[0] = '\\';
    out[1] = static_cast<int8_t>(second);
    return 2;
}

/// Write a JSON string value: `"` + content + `"`, escaping where required. Fast path: even the
/// worst-case escape expansion (6x, \u00XX) fits the remaining main-buffer space -- virtually
/// always mid-buffer -- so escape straight into the output buffer in clean-run chunks (no
/// intermediate std::string). Degenerates to quote + one memcpy + quote when nothing needs
/// escaping. Slow path only when the value might span into a child buffer: build the escaped
/// string and let writeBytesToBuffer handle the spilling.
uint64_t writeJsonStringValue(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* content,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const char* const data = reinterpret_cast<const char*>(content);
    if (tupleBuffer->getNumberOfChildBuffers() == 0 && contentSize * 6 + 2 <= remainingSpace)
    {
        int8_t* out = bufferStartingAddress;
        *out++ = static_cast<int8_t>('"');
        uint64_t runStart = 0;
        for (uint64_t i = 0; i < contentSize; ++i)
        {
            const unsigned char character = static_cast<unsigned char>(data[i]);
            if (character == '"' || character == '\\' || character < 0x20)
            {
                std::memcpy(out, data + runStart, i - runStart);
                out += i - runStart;
                out += writeEscapeSequence(out, data[i]);
                runStart = i + 1;
            }
        }
        std::memcpy(out, data + runStart, contentSize - runStart);
        out += contentSize - runStart;
        *out++ = static_cast<int8_t>('"');
        return static_cast<uint64_t>(out - bufferStartingAddress);
    }
    /// Slow path: the value is near the main-buffer boundary (or children already exist).
    std::string escaped;
    escaped.reserve(contentSize + 2);
    escaped.push_back('"');
    for (uint64_t i = 0; i < contentSize; ++i)
    {
        appendJsonEscaped(escaped, data[i]);
    }
    escaped.push_back('"');
    return writeBytesToBuffer(escaped.data(), escaped.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Escape-write one rope segment into the output buffer, optionally emitting a leading and/or trailing
/// double-quote. A rope's JSON string is `"` + escaped(seg0) + escaped(seg1) + ... + `"`; each segment is
/// escaped independently (a multi-byte JSON escape never straddles a segment seam, so per-segment escaping is
/// correct) and the first/last segment carries the delimiters, so the whole value is emitted with exactly one
/// invoke per segment and NO intermediate arena buffer. This is writeJsonStringValue generalised to a segment
/// that may be interior to a larger string; the single-span case still uses writeJsonStringValue verbatim.
uint64_t writeJsonStringSpan(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* content,
    const uint64_t contentSize,
    const bool openQuote,
    const bool closeQuote,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    const char* const data = reinterpret_cast<const char*>(content);
    const uint64_t quoteBytes = (openQuote ? 1U : 0U) + (closeQuote ? 1U : 0U);
    if (tupleBuffer->getNumberOfChildBuffers() == 0 && contentSize * 6 + quoteBytes <= remainingSpace)
    {
        int8_t* out = bufferStartingAddress;
        if (openQuote)
        {
            *out++ = static_cast<int8_t>('"');
        }
        uint64_t runStart = 0;
        for (uint64_t i = 0; i < contentSize; ++i)
        {
            const unsigned char character = static_cast<unsigned char>(data[i]);
            if (character == '"' || character == '\\' || character < 0x20)
            {
                std::memcpy(out, data + runStart, i - runStart);
                out += i - runStart;
                out += writeEscapeSequence(out, data[i]);
                runStart = i + 1;
            }
        }
        std::memcpy(out, data + runStart, contentSize - runStart);
        out += contentSize - runStart;
        if (closeQuote)
        {
            *out++ = static_cast<int8_t>('"');
        }
        return static_cast<uint64_t>(out - bufferStartingAddress);
    }
    /// Slow path: near the main-buffer boundary (or children already exist). Build the escaped chunk with the
    /// requested quotes and let writeBytesToBuffer spill it into child buffers correctly.
    std::string escaped;
    escaped.reserve(contentSize + 2);
    if (openQuote)
    {
        escaped.push_back('"');
    }
    for (uint64_t i = 0; i < contentSize; ++i)
    {
        appendJsonEscaped(escaped, data[i]);
    }
    if (closeQuote)
    {
        escaped.push_back('"');
    }
    return writeBytesToBuffer(escaped.data(), escaped.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Write a single CHAR as a JSON string ("x", escaped if needed).
uint64_t writeJsonChar(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const char content,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    std::string escaped;
    escaped.reserve(8);
    escaped.push_back('"');
    appendJsonEscaped(escaped, content);
    escaped.push_back('"');
    return writeBytesToBuffer(escaped.data(), escaped.size(), remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

/// Unchecked copy: the whole record has been verified to fit the main buffer, so this write is
/// guaranteed in-bounds -- no per-write fit guard, no child-buffer spill path (contrast
/// writeBytesToBuffer). Reads no static/thread_local state, so it is safe to splice via NAUTILUS_INLINE.
NAUTILUS_INLINE uint64_t copyBytesUnchecked(int8_t* destination, const int8_t* source, const uint64_t size)
{
    std::memcpy(destination, source, size);
    return size;
}

/// Emit host-known constant glue bytes on the already-fits-verified path: same folded-constant source as
/// writeConstantBytes (embedConstantBytes materialises the bytes into the JIT module when trace_constant_bytes
/// is on), but copied unchecked -- no fit guard, no spill. Returns the number of bytes written.
nautilus::val<uint64_t> writeConstantBytesUnchecked(const std::string& constant, const nautilus::val<int8_t*>& destination)
{
    const auto source = traceConstantBytesEnabled() ? nautilus::embedConstantBytes(constant.data(), constant.size())
                                                    : nautilus::val<const int8_t*>{reinterpret_cast<const int8_t*>(constant.data())};
    return nautilus::invoke(copyBytesUnchecked, destination, source, nautilus::val<uint64_t>{constant.size()});
}

/// The largest decimal text a NUMERIC type can occupy, rounded UP to a wide-store width (16 or 32),
/// both within the SIMDCSV indexer's 64-byte input over-read slack. Used as the fixed copy width for
/// the bounded value store below: int32 "-2147483648"=11, int64/uint64 ...=20, double shortest ~24.
/// Non-numeric types never reach here (declined host-side).
constexpr uint64_t maxTextWidth(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
            return 16;
        case DataType::Type::INT64:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return 32;
        default:
            return 0;
    }
}

/// Room to reserve for a COMPUTED value (not lazy passthrough -- it must be serialized). We can't know the
/// digit count without serializing, so reserve the CONFIGURED parser's own worst case, obtained from its
/// maxOutputWidth() (ZMIJ double = 25, fixed-6 double = 344, int64 = 20, ...). Using the parser's real
/// bound rather than a universal ceiling keeps float-heavy schemas viable in small buffers while staying
/// safe against over-write. Host-time only (one registry lookup per computed field per query compile).
uint64_t reservedWidthFor(const std::string& parserType)
{
    constexpr OutputParserRegistryArguments arguments{};
    if (const auto parser = OutputParserRegistry::instance().create(parserType, arguments))
    {
        return parser.value()->maxOutputWidth();
    }
    return 344;
}

/// A large compile-time-constant `remainingSpace` passed to the output parser on the reserved fast path.
/// Each serializer's fast path is `if (remainingSpace >= maxWidth) direct-write`, so a constant that
/// dominates every maxWidth folds that guard to constant-true -- the per-value fit-check and its spill
/// branch drop out, leaving only the direct serialize. Safe ONLY because the caller has already reserved
/// maxSerializedWidth for the field (the real bound); this sentinel just removes the redundant runtime check.
constexpr uint64_t kReservedRemainingSentinel = 1U << 20;

/// Bounded value store: copy a COMPILE-TIME-CONSTANT width via __builtin_memcpy_inline (lowers to fixed
/// wide stores -- NO `call memcpy`, the whole point) and advance the write cursor by the ACTUAL length.
/// The caller has proven (host + one runtime check) that every value is <= its width (no truncation),
/// that the destination has the width reserved (over-write of the slop is overwritten by the next field
/// / harmless past the record end), and that the source over-read <= the width stays inside the input
/// buffer's over-read slack. Two widths cover all numerics; the caller picks host-side (no runtime branch).
NAUTILUS_INLINE uint64_t copyBounded16(int8_t* destination, const int8_t* source, const uint64_t size)
{
    __builtin_memcpy_inline(destination, source, 16);
    return size;
}

NAUTILUS_INLINE uint64_t copyBounded32(int8_t* destination, const int8_t* source, const uint64_t size)
{
    __builtin_memcpy_inline(destination, source, 32);
    return size;
}

/// Host dispatch to the right bounded-store width for a NUMERIC type; `width` is host-known so this
/// selects the instantiation at trace time (no runtime branch). Returns the ACTUAL bytes written.
nautilus::val<uint64_t> emitBoundedValue(
    const uint64_t width,
    const nautilus::val<int8_t*>& destination,
    const nautilus::val<int8_t*>& content,
    const nautilus::val<uint64_t>& size)
{
    if (width <= 16)
    {
        return nautilus::invoke(copyBounded16, destination, content, size);
    }
    return nautilus::invoke(copyBounded32, destination, content, size);
}

/// Write raw pass-through bytes (lazy numeric/bool values: the source text is already a valid
/// JSON number/literal, so copy it verbatim).
NAUTILUS_INLINE uint64_t writeRawBytes(
    int8_t* bufferStartingAddress,
    const uint64_t remainingSpace,
    const int8_t* content,
    const uint64_t contentSize,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    return writeBytesToBuffer(
        reinterpret_cast<const char*>(content), contentSize, remainingSpace, tupleBuffer, bufferProvider, bufferStartingAddress);
}

void writeValue(
    const DataType& fieldType,
    const VarVal& value,
    const nautilus::val<int8_t*>& fieldPointer,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    nautilus::val<uint64_t>& written,
    nautilus::val<uint64_t>& currentRemainingSize,
    const std::string& parserType)
{
    switch (fieldType.type)
    {
        case DataType::Type::BOOLEAN: {
            /// JSON enforces true/false literals, so lazy bools are parsed (never passed through raw:
            /// the CSV source text may be 1/0).
            static const std::string trueLiteral = "true";
            static const std::string falseLiteral = "false";
            const auto boolValue = value.getRawValueAs<nautilus::val<bool>>();
            nautilus::val<uint64_t> amountWritten{0};
            if (boolValue)
            {
                amountWritten = writeConstantBytes(trueLiteral, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
            }
            else
            {
                amountWritten
                    = writeConstantBytes(falseLiteral, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
            }
            written += amountWritten;
            currentRemainingSize -= amountWritten;
            break;
        }
        case DataType::Type::CHAR: {
            if (value.isLazyValue())
            {
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeJsonStringValue,
                    fieldPointer + written,
                    currentRemainingSize,
                    lazyValue->getContent(),
                    lazyValue->getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            else
            {
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeJsonChar,
                    fieldPointer + written,
                    currentRemainingSize,
                    value.getRawValueAs<nautilus::val<char>>(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            break;
        }
        case DataType::Type::VARSIZED: {
            if (value.isLazyValue())
            {
                /// Lazy VARSIZED: walk the value's byte spans and write them straight to the output. A rope
                /// (e.g. a CONCAT result) is a multi-span list -- writing the spans directly avoids the arena
                /// round-trip (materialise-then-recopy); a passthrough field is the single-span degenerate case.
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                if (not lazyValue->isRope())
                {
                    /// Single-span passthrough (a column view): write its contiguous bytes directly. isRope() is
                    /// host-only, so this avoids calling getSpans() on the hot passthrough path (which touches SSA
                    /// handles); getContent()/getSize() give the same view without materialising.
                    const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                        writeJsonStringValue,
                        fieldPointer + written,
                        currentRemainingSize,
                        lazyValue->getContent(),
                        lazyValue->getSize(),
                        recordBuffer.getReference(),
                        bufferProvider);
                    written += amountWritten;
                    currentRemainingSize -= amountWritten;
                }
                else
                {
                    const auto spans = lazyValue->getSpans();
                    /// Multi-span rope: `"` + escaped(seg0) + ... + escaped(segN-1) + `"`, one invoke per span,
                    /// host-unrolled (the span count is compile-time). When the formatter is FUSED into the source
                    /// pipeline (single-node worker), the rope reaches here and its segments are written straight
                    /// into the JSON output buffer -- input->output, one copy, no arena or child buffer.
                    const std::size_t spanCount = spans.size();
                    for (nautilus::static_val<std::size_t> spanIndex = 0; spanIndex < spanCount; ++spanIndex)
                    {
                        const std::size_t index = spanIndex;
                        const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                            writeJsonStringSpan,
                            fieldPointer + written,
                            currentRemainingSize,
                            spans[index].ptr,
                            spans[index].len,
                            nautilus::val<bool>{index == 0},
                            nautilus::val<bool>{index + 1 == spanCount},
                            recordBuffer.getReference(),
                            bufferProvider);
                        written += amountWritten;
                        currentRemainingSize -= amountWritten;
                    }
                }
            }
            else
            {
                const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeJsonStringValue,
                    fieldPointer + written,
                    currentRemainingSize,
                    varSizedValue.getContent(),
                    varSizedValue.getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            break;
        }
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64: {
            if (value.isLazyValue())
            {
                /// The source text of a numeric field is already a valid JSON number: copy it raw.
                const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
                const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                    writeRawBytes,
                    fieldPointer + written,
                    currentRemainingSize,
                    lazyValue->getContent(),
                    lazyValue->getSize(),
                    recordBuffer.getReference(),
                    bufferProvider);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            else
            {
                const nautilus::val<uint64_t> amountWritten
                    = formatAndWriteVal(value, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider, parserType);
                written += amountWritten;
                currentRemainingSize -= amountWritten;
            }
            break;
        }
        case DataType::Type::UNDEFINED: {
            throw UnknownDataType("JSON-OutputFormatting for type UNDEFINED is not supported.");
        }
    }
}
}

JSONOutputFormatter::JSONOutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames, descriptor)
{
    fieldPrefixes.reserve(fieldNames.size());
    for (size_t fieldIndex = 0; fieldIndex < fieldNames.size(); ++fieldIndex)
    {
        fieldPrefixes.push_back(fmt::format("{}\"{}\":", fieldIndex == 0 ? "{" : ",", jsonEscapeString(fieldNames[fieldIndex])));
    }
}

nautilus::val<uint64_t> JSONOutputFormatter::writeFormattedValue(
    const VarVal& value,
    const DataType& fieldType,
    const nautilus::static_val<uint64_t>& fieldIndex,
    const nautilus::val<int8_t*>& fieldPointer,
    const nautilus::val<uint64_t>& remainingSize,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    nautilus::val<uint64_t> written{0};
    nautilus::val<uint64_t> currentRemainingSize = remainingSize;

    /// Pre-value glue (`{"name":` for the first field, `,"name":` otherwise): precomputed at
    /// construction and emitted as immediate stores -- the JIT sees the bytes, not a pointer
    /// (see writeConstantBytes for why that is the better codegen).
    const std::string& prefix = fieldPrefixes.at(fieldIndex);
    const nautilus::val<uint64_t> prefixWritten
        = writeConstantBytes(prefix, fieldPointer, currentRemainingSize, recordBuffer, bufferProvider);
    written += prefixWritten;
    currentRemainingSize -= prefixWritten;

    /// Handle NULL values and write value
    if (value.isNullable() && value.isNull())
    {
        static const std::string nullLiteral = "null";
        const nautilus::val<uint64_t> amountWritten
            = writeConstantBytes(nullLiteral, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
        written += amountWritten;
        currentRemainingSize -= amountWritten;
    }
    else
    {
        writeValue(
            fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes.at(fieldType.type));
    }

    /// The record suffix after the last field; interior fields need no trailing delimiter (the next
    /// field's prefix carries the comma). The field index is host-known, so this is compile-time
    /// control flow -- no runtime select.
    if (static_cast<uint64_t>(fieldIndex) == fieldNames.size() - 1)
    {
        static const std::string recordSuffix = "}\n";
        written += writeConstantBytes(recordSuffix, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
    }
    return written;
}

std::optional<nautilus::val<uint64_t>> JSONOutputFormatter::tryWriteRecordAllPassthrough(
    const Record& record,
    const std::vector<DataType>& fieldTypes,
    const nautilus::val<int8_t*>& recordAddress,
    const nautilus::val<uint64_t>& remainingMainBytes,
    const RecordBuffer& recordBuffer,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    const std::size_t numberOfFields = fieldNames.size();

    /// Host-time decline: JSON only passes a lazy value through verbatim when it is NUMERIC (the source
    /// text is already a valid JSON number). Lazy bools become true/false literals and lazy char/varsized
    /// are quoted+escaped, so any non-numeric field must take the per-field writeFormattedValue() path.
    for (const auto& fieldType : fieldTypes)
    {
        if (not fieldType.isNumeric())
        {
            return std::nullopt;
        }
    }

    /// Single host-side pass: read each field's value ONCE (re-reading a lazy value in the emit loop
    /// re-traces getContent/getSize and trips a nautilus "constant loop" trace error), storing what the
    /// emit needs, and accumulate the RESERVATION size + whether every LAZY value fits its bounded-copy
    /// width. A field is either a lazy passthrough value (reserve its exact runtime length; emit via the
    /// bounded fixed-width store) or a COMPUTED value that must be serialized (reserve the configured
    /// parser's maxOutputWidth, its worst case; emit via the parser's direct-write). Whether a value is
    /// lazy is a host/trace-time property, so the per-field branch is compile-time control flow. `contents`/`sizes`
    /// are optional so computed fields store nothing there (no dummy-constant emission in this host loop).
    static const std::string recordSuffix = "}\n";
    std::vector<VarVal> values;
    values.reserve(numberOfFields);
    std::vector<std::optional<nautilus::val<int8_t*>>> contents(numberOfFields);
    std::vector<std::optional<nautilus::val<uint64_t>>> sizes(numberOfFields);
    std::vector<bool> isLazy(numberOfFields);
    uint64_t glueTotal = recordSuffix.size();
    nautilus::val<uint64_t> reserveTotal{0};
    nautilus::val<bool> allWithinBound{true};
    for (std::size_t fieldIndex = 0; fieldIndex < numberOfFields; ++fieldIndex)
    {
        glueTotal += fieldPrefixes.at(fieldIndex).size();
        auto value = record.read(fieldNames.at(fieldIndex));
        isLazy[fieldIndex] = value.isLazyValue();
        if (isLazy[fieldIndex])
        {
            const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
            contents[fieldIndex] = lazyValue->getContent();
            sizes[fieldIndex] = lazyValue->getSize();
            reserveTotal += *sizes[fieldIndex];
            allWithinBound
                = allWithinBound and (*sizes[fieldIndex] <= nautilus::val<uint64_t>{maxTextWidth(fieldTypes.at(fieldIndex).type)});
        }
        else
        {
            reserveTotal += nautilus::val<uint64_t>{reservedWidthFor(parserTypes.at(fieldTypes.at(fieldIndex).type))};
        }
        values.push_back(value);
    }

    /// FAST PATH: every lazy value fits its bounded width AND the reservation (exact lazy lengths +
    /// worst-case computed widths + glue) plus over-write slack fits the main buffer. Then a single
    /// record-level check replaces every per-field fit-guard: lazy values are emitted with a fixed-width
    /// __builtin_memcpy_inline (no call memcpy), and computed values are serialized directly with the
    /// serializer's own fit-check folded away (kReservedRemainingSentinel). The serializer writes the
    /// actual digits and the cursor advances by that; the bounded copy's slop stays within the reserved
    /// slack (overwritten by the next field or harmless past the record end).
    constexpr uint64_t kOverWriteSlack = 32;
    nautilus::val<uint64_t> written{0};
    /// The reservation MUST include glueTotal (the field prefixes + the "}\n" suffix that the emit loop
    /// writes via writeConstantBytesUnchecked), not just the field values. Omitting it under-reserves by the
    /// glue size, so a record landing within glueTotal bytes of the main-buffer end passes this check and then
    /// the unchecked writes run past bufferEnd, corrupting the adjacent buffer's control block (observed as a
    /// garbage refcount from the BufferManager). glueTotal + reserveTotal + kOverWriteSlack is the true
    /// worst-case physical footprint; records that don't fit it fall to the boundary-safe per-field fallback.
    if (allWithinBound and (reserveTotal + nautilus::val<uint64_t>{glueTotal + kOverWriteSlack} <= remainingMainBytes))
    {
        for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
        {
            const std::size_t fieldIndex = i;
            written += writeConstantBytesUnchecked(fieldPrefixes.at(fieldIndex), recordAddress + written);
            if (isLazy[fieldIndex])
            {
                written += emitBoundedValue(
                    maxTextWidth(fieldTypes.at(fieldIndex).type), recordAddress + written, *contents[fieldIndex], *sizes[fieldIndex]);
            }
            else
            {
                written += formatAndWriteVal(
                    values[fieldIndex],
                    recordAddress + written,
                    nautilus::val<uint64_t>{kReservedRemainingSentinel},
                    recordBuffer,
                    bufferProvider,
                    parserTypes.at(fieldTypes.at(fieldIndex).type));
            }
        }
        written += writeConstantBytesUnchecked(recordSuffix, recordAddress + written);
        return written;
    }

    /// FALLBACK: a value exceeded its bounded width (pathologically long field), or the record is near
    /// the main-buffer boundary. Delegate per-field to the canonical writeFormattedValue(), which emits
    /// exact copies and spills into child buffers correctly -- no duplicated emit logic here.
    nautilus::val<uint64_t> currentRemaining = remainingMainBytes;
    for (nautilus::static_val<uint64_t> i = 0; i < numberOfFields; ++i)
    {
        const std::size_t fieldIndex = i;
        const auto amountWritten = writeFormattedValue(
            record.read(fieldNames.at(fieldIndex)),
            fieldTypes.at(fieldIndex),
            i,
            recordAddress + written,
            currentRemaining,
            recordBuffer,
            bufferProvider);
        written += amountWritten;
        currentRemaining -= amountWritten;
    }
    return written;
}

DescriptorConfig::Config JSONOutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersJSON>(std::move(config), "JSON");
}

std::ostream& operator<<(std::ostream& out, const JSONOutputFormatter&)
{
    return out << fmt::format("JSONOutputFormatter()");
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterJSONOutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return JSONOutputFormatter::validateAndFormat(std::move(args.config));
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterJSONOutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<JSONOutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}
}
