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

#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <SchemaJSONFieldIndex.hpp>
#include <static.hpp>

namespace NES
{

/// SchemaJSON -- a JSON input formatter that reuses simdjson's SIMD structural indexer ("stage 1") as its
/// indexing phase, WITHOUT simdjson's DOM / On-Demand navigation layers. It exploits a deliberately rigid
/// schema to turn value extraction into a fixed-stride gather over the structural-index stream.
///
/// Schema assumptions (rejected at index time if violated in a way that changes the structural count):
///   - every record is a FLAT JSON object with EXACTLY the schema's fields, in the SAME order;
///   - no missing keys, no extra keys, no `null`-as-absent, no nested objects/arrays as values.
/// A flat object of F fields emits exactly S = 4F + 1 structural indexes, and value f is the entry at local
/// slot 3 + 4f (the entry immediately following the f-th ':'). Its byte range ends at the next entry (the
/// ',' or closing '}'). This is the whole trick: under the fixed schema, value positions live at a fixed
/// stride, so we address them directly instead of navigating the document.
///
/// Unlike the "JSON" formatter (SIMDJSONInputFormatIndexer, which uses simdjson On-Demand and therefore
/// tolerates reordered/missing/extra keys and nested values), SchemaJSON trades that flexibility for the
/// speed of a strided gather. Point it only at data that matches the schema shape above.
struct ConfigParametersSchemaJSON
{
    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
};

/// PROTOTYPE (option 2): SchemaJSON uses a custom FieldIndexFunction (SchemaJSONFieldIndex) that reads
/// simdjson's stage-1 structural indexes directly at read time -- no separate FieldOffsets band, no gather.
struct SchemaJSONMetaData
{
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;

    explicit SchemaJSONMetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
        : tupleDelimiter(config.getFromConfig(ConfigParametersSchemaJSON::TUPLE_DELIMITER))
        , fieldNames(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , nullValues({"null"})
    {
        PRECONDITION(
            tupleDelimiter.size() == SIZE_OF_TUPLE_DELIMITER,
            "Delimiters must be of size '1 byte', but the tuple delimiter was {} (size {})",
            tupleDelimiter,
            tupleDelimiter.size());
        PRECONDITION(fieldNames.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
    }

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const { return tupleDelimiter; }

    /// JSON string values are double-quoted; the raw value parser strips the surrounding quotes per field.
    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(i < fieldNames.size(), "Trying to access position, larger than the size of fieldNames {}", fieldNames.size());
        return fieldNames[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const
    {
        PRECONDITION(
            i < fieldDataTypes.size(), "Trying to access position, larger than the size of fieldDataTypes {}", fieldDataTypes.size());
        return fieldDataTypes[i];
    }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNames.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNames.size();
    }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const { return nullValues; }

private:
    std::string tupleDelimiter;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues;
};

/// Runs simdjson stage 1 over `region` (the bytes of all COMPLETE records in the raw buffer, i.e. between the
/// first and last tuple delimiter) into this FIF's thread_local-pooled dom_parser_implementation and records
/// the (non-owning) structural-index base + record count on `fieldIndex`. NO gather, NO FieldOffsets band.
/// `base` is the absolute offset of `region` within the raw buffer (added at read time). `numFields` is the
/// schema arity F. Defined in SchemaJSONStage1.cpp so that simdjson stays confined to a single TU.
void schemaJsonIndexIntoFieldIndex(SchemaJSONFieldIndex& fieldIndex, std::string_view region, FieldIndex base, uint32_t numFields);

class SchemaJSONInputFormatIndexer final : public InputFormatIndexer<SchemaJSONInputFormatIndexer>
{
public:
    static constexpr std::string_view NAME = "SchemaJSON";
    static constexpr bool IsSequential = false;
    static constexpr char TUPLE_DELIMITER = '\n';

    using IndexerMetaData = SchemaJSONMetaData;
    using FieldIndexFunctionType = SchemaJSONFieldIndex;

    SchemaJSONInputFormatIndexer() = default;
    ~SchemaJSONInputFormatIndexer() = default;

    void indexRawBuffer(SchemaJSONFieldIndex& fieldIndex, const RawTupleBuffer& rawBuffer, const SchemaJSONMetaData& metaData) const;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    friend std::ostream& operator<<(std::ostream& os, const SchemaJSONInputFormatIndexer& indexer);
};

}
