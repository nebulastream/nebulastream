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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// The output formatter is responsible for converting a Record into a string of a given Output Format like CSV or JSON
/// Output formatters are stateless, meaning that they must be able to produce valid output in a streaming fashion.
/// Output formatters are used by the OutputFormatterBufferRef during the last emit before a sink.
class OutputFormatter
{
public:
    explicit OutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor)
        : fieldNames(fieldNames)
    {
        INVARIANT(!fieldNames.empty(), "Schema is not allowed to have 0 fields");
        parserTypes[DataType::Type::BOOLEAN] = descriptor.getFromConfig(OutputFormatterDescriptor::BOOL_PARSER);
        parserTypes[DataType::Type::CHAR] = descriptor.getFromConfig(OutputFormatterDescriptor::CHAR_PARSER);
        parserTypes[DataType::Type::UINT8] = descriptor.getFromConfig(OutputFormatterDescriptor::UINT8_PARSER);
        parserTypes[DataType::Type::UINT16] = descriptor.getFromConfig(OutputFormatterDescriptor::UINT16_PARSER);
        parserTypes[DataType::Type::UINT32] = descriptor.getFromConfig(OutputFormatterDescriptor::UINT32_PARSER);
        parserTypes[DataType::Type::UINT64] = descriptor.getFromConfig(OutputFormatterDescriptor::UINT64_PARSER);
        parserTypes[DataType::Type::INT8] = descriptor.getFromConfig(OutputFormatterDescriptor::INT8_PARSER);
        parserTypes[DataType::Type::INT16] = descriptor.getFromConfig(OutputFormatterDescriptor::INT16_PARSER);
        parserTypes[DataType::Type::INT32] = descriptor.getFromConfig(OutputFormatterDescriptor::INT32_PARSER);
        parserTypes[DataType::Type::INT64] = descriptor.getFromConfig(OutputFormatterDescriptor::INT64_PARSER);
        parserTypes[DataType::Type::FLOAT32] = descriptor.getFromConfig(OutputFormatterDescriptor::F32_PARSER);
        parserTypes[DataType::Type::FLOAT64] = descriptor.getFromConfig(OutputFormatterDescriptor::F64_PARSER);
        /// These types should never require a parser but we set something for them anyway, as the parser type is accessed before we know that
        /// the type actually has a parser
        parserTypes[DataType::Type::VARSIZED] = "";
        parserTypes[DataType::Type::UNDEFINED] = "";
    }

    virtual ~OutputFormatter() noexcept = default;

    /// Format the field's contents into a string and write the string bytes into the field pointer address.
    /// If the formatted value does not fit into the record buffer, child buffers will be allocated to write the value into.
    /// The main buffer's space will always be utilized completely before writing in a child.
    /// Returns the number of bytes written into the record buffer (excluding the bytes written into the children).
    [[nodiscard]] virtual nautilus::val<uint64_t> writeFormattedValue(
        const VarVal& value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
        = 0;

    /// Host-time predicate: may the caller group contiguous non-nullable lazy passthrough fields into runs and
    /// offer them to tryWriteCoalescedRun()? Only formatters that ALWAYS accept such a run (never return
    /// nullopt for it) should return true, so the caller can safely skip a run's interior fields. Default: no.
    [[nodiscard]] virtual bool supportsRunCoalescing() const { return false; }

    /// Optional coalesced fast path for a RUN of adjacent non-nullable lazy passthrough fields (raw input
    /// bytes). The caller groups maximal contiguous runs of such fields and offers each here; `runFieldNames`
    /// is the run's fields in order and `runEndsRecord` is true iff the run's last field is the record's last
    /// field (=> a tuple delimiter follows, else a field delimiter). Returns the bytes written into the main
    /// buffer, or std::nullopt to decline -- in which case the caller formats the run field-by-field via
    /// writeFormattedValue(). A formatter overrides this to emit the run as ONE copy (the in-between field
    /// delimiters ride along) when the fields are actually adjacent in the source buffer. Default: decline.
    [[nodiscard]] virtual std::optional<nautilus::val<uint64_t>> tryWriteCoalescedRun(
        const Record&,
        const std::vector<Record::RecordFieldIdentifier>& /*runFieldNames*/,
        bool /*runEndsRecord*/,
        const nautilus::val<int8_t*>& /*recordAddress*/,
        const nautilus::val<uint64_t>& /*remainingSize*/,
        const RecordBuffer&,
        const nautilus::val<AbstractBufferProvider*>&) const
    {
        return std::nullopt;
    }

    /// Optional whole-record fast path for records whose fields are ALL non-nullable lazy passthrough
    /// values (raw input bytes). The caller (OutputFormatterBufferRef) verifies that precondition host-side
    /// and passes the matching `fieldTypes` plus the bytes remaining in the MAIN buffer at `recordAddress`.
    /// A formatter may still DECLINE host-side (return nullopt) if raw passthrough is not valid for some
    /// field type under its format -- e.g. JSON only passes NUMERIC lazy values through verbatim (it parses
    /// lazy bools to true/false and quotes lazy char/varsized). When it accepts, it computes the record's
    /// EXACT output size in one pass (host-constant glue plus the runtime lazy field lengths) and emits the
    /// whole record behind a SINGLE hoisted fit-check: if it fits the main buffer, unchecked copies (no
    /// per-field guard, no child-buffer spill); otherwise it falls back internally to guarded writes that
    /// spill correctly. Returns the bytes written into the main buffer. This is the record-level analogue of
    /// tryWriteCoalescedRun for formatters that cannot coalesce (e.g. JSON, whose per-field glue prevents a
    /// single contiguous copy). Default: decline.
    [[nodiscard]] virtual std::optional<nautilus::val<uint64_t>> tryWriteRecordAllPassthrough(
        const Record&,
        const std::vector<DataType>& /*fieldTypes*/,
        const nautilus::val<int8_t*>& /*recordAddress*/,
        const nautilus::val<uint64_t>& /*remainingMainBytes*/,
        const RecordBuffer&,
        const nautilus::val<AbstractBufferProvider*>&) const
    {
        return std::nullopt;
    }

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const OutputFormatter& obj);

protected:
    /// Identifiers of the fields of the output schema
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    /// Stores the configured parser for each datatype.
    std::unordered_map<DataType::Type, std::string> parserTypes;
};

}

template <std::derived_from<NES::OutputFormatter> OutputFormatter>
struct fmt::formatter<OutputFormatter> : fmt::ostream_formatter
{
};
