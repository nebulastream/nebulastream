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
#include <OutputFormatters/OutputFormatter.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/core.h>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
class CSVOutputFormatter : public OutputFormatter
{
public:
    explicit CSVOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames, const OutputFormatterDescriptor& descriptor);

    [[nodiscard]] nautilus::val<uint64_t> writeFormattedValue(
        const VarVal& value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    /// Coalesced fast path for a RUN of contiguous non-nullable lazy passthrough fields: if the field
    /// delimiter is a single byte and the run's fields are actually adjacent in the source buffer -- checked
    /// at runtime, with the in-between byte verified to be the field delimiter -- the run is emitted as ONE
    /// copy (the in-between field delimiters ride along) plus the trailing delimiter (tuple if `runEndsRecord`,
    /// else field). Runtime-non-contiguous runs fall back to per-field lazy byte copies (still no parse). Only
    /// the multi-byte-delimiter case declines (nullopt). Generalises the previous whole-record coalescing so a
    /// projection that MIXES computed and passthrough columns still coalesces each contiguous passthrough run.
    /// A single-byte field delimiter is the only condition under which tryWriteCoalescedRun accepts a run, so
    /// runs are only formed then -- guaranteeing no run's interior fields are skipped without being written.
    [[nodiscard]] bool supportsRunCoalescing() const override { return fieldDelimiter.size() == 1; }

    [[nodiscard]] std::optional<nautilus::val<uint64_t>> tryWriteCoalescedRun(
        const Record& record,
        const std::vector<Record::RecordFieldIdentifier>& runFieldNames,
        bool runEndsRecord,
        const nautilus::val<int8_t*>& recordAddress,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    /// validates and formats a string to string configuration
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& out, const CSVOutputFormatter& format);

private:
    bool quoteStrings;
    std::string fieldDelimiter;
    std::string tupleDelimiter;
};
}

namespace NES::OutputFormatterConfig
{
struct ConfigParametersCSV
{
    static inline const DescriptorConfig::ConfigParameter<bool> QUOTE_STRINGS{
        "quote_strings",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(QUOTE_STRINGS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FIELD_DELIMITER{
        "field_delimiter",
        ",",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FIELD_DELIMITER, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            OutputFormatterDescriptor::parameterMap, QUOTE_STRINGS, FIELD_DELIMITER, TUPLE_DELIMITER);
};
}

FMT_OSTREAM(NES::OutputFormatter);
