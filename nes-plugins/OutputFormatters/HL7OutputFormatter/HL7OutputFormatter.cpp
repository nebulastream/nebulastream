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

#include <HL7OutputFormatter.hpp>

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/LazyValueRepresentation.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <OutputFormatterRegistry.hpp>
#include <OutputFormatterValidationRegistry.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// Write raw pass-through bytes: HL7 fields carry the value's text verbatim (no quoting, no
/// escaping -- see the class comment for the deliberate no-escape simplification), so every lazy
/// value AND every materialized varsized value is a plain byte copy. NAUTILUS_INLINE so the copy
/// is spliced into the JIT pipeline (registered via nautilus_inline in this plugin's CMakeLists).
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
    if (fieldType.type == DataType::Type::UNDEFINED)
    {
        throw UnknownDataType("HL7-OutputFormatting for type UNDEFINED is not supported.");
    }
    if (value.isLazyValue())
    {
        if (fieldType.type == DataType::Type::VARSIZED)
        {
            /// Ask the value to write itself one contiguous run at a time. writeEachChunk drives the
            /// walk and hides whether it is a single passthrough span or a many-segment concat rope;
            /// each run goes straight into the output buffer (input->output, one copy, no arena
            /// round-trip, no escape scan).
            const auto lazyValue = value.getRawValueAs<std::shared_ptr<LazyValueRepresentation>>();
            lazyValue->writeEachChunk(
                [&](const nautilus::val<int8_t*>& chunkPtr, const nautilus::val<uint64_t>& chunkLen, const bool, const bool)
                {
                    const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
                        writeRawBytes,
                        fieldPointer + written,
                        currentRemainingSize,
                        chunkPtr,
                        chunkLen,
                        recordBuffer.getReference(),
                        bufferProvider);
                    written += amountWritten;
                    currentRemainingSize -= amountWritten;
                });
            return;
        }
        /// Numeric/bool/char lazy values: the source text is already a valid HL7 field body.
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
        return;
    }
    if (fieldType.type == DataType::Type::VARSIZED)
    {
        const auto varSizedValue = value.getRawValueAs<VariableSizedData>();
        const nautilus::val<uint64_t> amountWritten = nautilus::invoke(
            writeRawBytes,
            fieldPointer + written,
            currentRemainingSize,
            varSizedValue.getContent(),
            varSizedValue.getSize(),
            recordBuffer.getReference(),
            bufferProvider);
        written += amountWritten;
        currentRemainingSize -= amountWritten;
        return;
    }
    /// Computed (non-lazy) fixed-size value: serialize with the configured output parser.
    const nautilus::val<uint64_t> amountWritten
        = formatAndWriteVal(value, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider, parserType);
    written += amountWritten;
    currentRemainingSize -= amountWritten;
}
}

HL7OutputFormatter::HL7OutputFormatter(
    const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
    : OutputFormatter(fieldNames, descriptor)
    , messageHeader(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::MESSAGE_HEADER))
    , segmentName(descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::SEGMENT_NAME))
{
    const auto frameStart = descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::FRAME_START);
    const auto segmentDelimiter = descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::SEGMENT_DELIMITER);
    const auto fieldDelimiter = descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::FIELD_DELIMITER);
    const auto frameEnd = descriptor.getFromConfig(OutputFormatterConfig::ConfigParametersHL7::FRAME_END);

    /// Field 0 carries the whole constant message prologue; every other field just the field
    /// delimiter. All glue is host-known, so it folds into the JIT'd pipeline as immediate stores.
    fieldPrefixes.reserve(fieldNames.size());
    fieldPrefixes.push_back(frameStart + messageHeader + segmentDelimiter + segmentName + fieldDelimiter);
    for (size_t fieldIndex = 1; fieldIndex < fieldNames.size(); ++fieldIndex)
    {
        fieldPrefixes.push_back(fieldDelimiter);
    }
    recordSuffix = segmentDelimiter + frameEnd;
}

nautilus::val<uint64_t> HL7OutputFormatter::writeFormattedValue(
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

    /// Pre-value glue: the message prologue for field 0, the field delimiter otherwise --
    /// precomputed at construction and emitted as immediate stores.
    const std::string& prefix = fieldPrefixes.at(fieldIndex);
    const nautilus::val<uint64_t> prefixWritten
        = writeConstantBytes(prefix, fieldPointer, currentRemainingSize, recordBuffer, bufferProvider);
    written += prefixWritten;
    currentRemainingSize -= prefixWritten;

    /// NULL emits zero bytes: HL7's empty-field convention (`||`).
    if (not(value.isNullable() && value.isNull()))
    {
        writeValue(
            fieldType, value, fieldPointer, recordBuffer, bufferProvider, written, currentRemainingSize, parserTypes.at(fieldType.type));
    }

    /// The message trailer (segment delimiter + MLLP <FS><CR>) after the last field; interior fields
    /// need no trailing delimiter (the next field's prefix carries it). The field index is
    /// host-known, so this is compile-time control flow.
    if (static_cast<uint64_t>(fieldIndex) == fieldNames.size() - 1)
    {
        written += writeConstantBytes(recordSuffix, fieldPointer + written, currentRemainingSize, recordBuffer, bufferProvider);
    }
    return written;
}

DescriptorConfig::Config HL7OutputFormatter::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// SQL string literals reach the config verbatim (the parser does no escape processing), so
    /// values carrying control bytes are written as escape sequences ('\r', '\x1C\r', '\x0B') and
    /// unescaped here. The C++-literal defaults never pass through this map. HL7's own escape
    /// sequences (\F\, \S\, ... -- uppercase) are not recognized forms and pass through unchanged.
    for (const auto* const key : {"message_header", "segment_name", "frame_start", "segment_delimiter", "field_delimiter", "frame_end"})
    {
        if (const auto entry = config.find(key); entry != config.end())
        {
            entry->second = unescapeSpecialCharacters(entry->second);
        }
    }
    return DescriptorConfig::validateAndFormat<OutputFormatterConfig::ConfigParametersHL7>(std::move(config), "HL7");
}

std::ostream& operator<<(std::ostream& out, const HL7OutputFormatter& format)
{
    return out << fmt::format("HL7OutputFormatter(messageHeader: {}, segmentName: {})", format.messageHeader, format.segmentName);
}

OutputFormatterValidationRegistryReturnType
OutputFormatterValidationGeneratedRegistrar::RegisterHL7OutputFormatterValidation(OutputFormatterValidationRegistryArguments args)
{
    return HL7OutputFormatter::validateAndFormat(std::move(args.config));
}

OutputFormatterRegistryReturnType OutputFormatterGeneratedRegistrar::RegisterHL7OutputFormatter(OutputFormatterRegistryArguments args)
{
    return std::make_unique<HL7OutputFormatter>(std::move(args.fieldNames), std::move(args.descriptor));
}
}
