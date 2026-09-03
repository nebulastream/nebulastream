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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ranges>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>

namespace NES
{

struct SIMDJSONInputFormatterConfig
{
    char tupleDelimiter;

    static std::expected<SIMDJSONInputFormatterConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Parses JSON-encoded tuples (one JSON object per tuple delimiter, '\n' by default) using
/// SIMDJSON's ondemand parser. Nested JSON objects are accessed by encoding the path in the
/// (quoted) schema field name with '/' as separator, e.g. "MILK/CYCLES/LEFT" reads
/// {"MILK": {"CYCLES": {"LEFT": ...}}}. Unquoted identifiers are uppercased, so JSON keys must be
/// uppercase to match; use quoted identifiers for case-sensitive keys. For nullable fields, a
/// missing field, a missing parent object, or an explicit JSON null all map to NULL; for NOT NULL
/// fields, a missing field/parent raises FieldNotFound. JSON arrays are not supported.
class SIMDJSONInputFormatIndexer final : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr std::string_view NAME = "JSON";
    static constexpr char DELIMITER_SIZE = sizeof(char);
    static constexpr char TUPLE_DELIMITER = '\n';
    static constexpr char KEY_VALUE_DELIMITER = ':';
    static constexpr char KEY_QUOTE = '"';

    explicit SIMDJSONInputFormatIndexer(
        Private,
        const char tupleDelimiter,
        std::vector<std::string> jsonPointersToFields,
        std::vector<Record::RecordFieldIdentifier> fieldNamesOutput,
        std::vector<DataType> fieldDataTypes)
        : tupleDelimiter(tupleDelimiter)
        , jsonPointersToFields(std::move(jsonPointersToFields))
        , fieldNamesOutput(std::move(fieldNamesOutput))
        , fieldDataTypes(std::move(fieldDataTypes))
        , nullValues({})
    {
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<SIMDJSONInputFormatIndexer>
    create(const SIMDJSONInputFormatterConfig& config, const TupleBufferRef& tupleBufferRef)
    {
        /// JSON keys are unqualified — take the trailing identifier of each (possibly source-qualified) name.
        /// Precompute each field's JSON Pointer (RFC 6901) once: prepend '/' and escape literal '~' as
        /// '~0'. A '/' in a (quoted) field name deliberately stays unescaped — it separates nesting levels.
        std::vector<std::string> jsonPointersToFields;
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            jsonPointersToFields.emplace_back("/" + replaceAll(std::ranges::rbegin(fieldName)->asCanonicalString(), "~", "~0"));
        }

        auto fieldNamesOutput = tupleBufferRef.getAllFieldNames();
        auto fieldDataTypes = tupleBufferRef.getAllDataTypes();
        PRECONDITION(jsonPointersToFields.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        PRECONDITION(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");

        return std::make_unique<SIMDJSONInputFormatIndexer>(
            Private{}, config.tupleDelimiter, std::move(jsonPointersToFields), std::move(fieldNamesOutput), std::move(fieldDataTypes));
    }

    ~SIMDJSONInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, 1}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return ""; }

    [[nodiscard]] QuotationType getQuotationType() const override { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(uint64_t fieldIndex) const { return fieldNamesOutput[fieldIndex]; }

    [[nodiscard]] std::string_view getJsonPointerAt(const nautilus::static_val<uint64_t>& fieldIndex) const
    {
        return jsonPointersToFields[fieldIndex];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(uint64_t fieldIndex) const { return fieldDataTypes[fieldIndex]; }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNamesOutput.size();
    }

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    std::vector<std::string> jsonPointersToFields;
    std::vector<Record::RecordFieldIdentifier> fieldNamesOutput;
    std::vector<DataType> fieldDataTypes;
    std::vector<std::string> nullValues;
};
}
