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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ranges>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <InputFormatterDescriptor.hpp>
#include <RawBufferIndex.hpp>
#include <ValueDeserializerUtil.hpp>
#include <static.hpp>

namespace NES
{
struct ConfigParametersSIMDJSON
{
    static inline const DescriptorConfig::ConfigParameter<char> TUPLE_DELIMITER{
        "TUPLE_DELIMITER",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<char>
        {
            const auto it = config.find("TUPLE_DELIMITER");
            if (it == config.end())
            {
                return '\n';
            }
            const auto unescaped = unescapeSpecialCharacters(it->second);
            return (unescaped.size() == 1) ? std::optional<char>{unescaped.front()} : std::nullopt;
        }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
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
        std::vector<DataType> fieldDataTypes,
        const std::string& deserializerOverrides)
        : tupleDelimiter(tupleDelimiter)
        , jsonPointersToFields(std::move(jsonPointersToFields))
        , fieldNamesOutput(std::move(fieldNamesOutput))
        , fieldDataTypes(std::move(fieldDataTypes))
        , nullValues({})
    {
        deserializerTypes[DataType::Type::UINT8] = "DefaultUINT8";
        deserializerTypes[DataType::Type::UINT16] = "DefaultUINT16";
        deserializerTypes[DataType::Type::UINT32] = "DefaultUINT32";
        deserializerTypes[DataType::Type::UINT64] = "DefaultUINT64";
        deserializerTypes[DataType::Type::INT8] = "DefaultINT8";
        deserializerTypes[DataType::Type::INT16] = "DefaultINT16";
        deserializerTypes[DataType::Type::INT32] = "DefaultINT32";
        deserializerTypes[DataType::Type::INT64] = "DefaultINT64";
        deserializerTypes[DataType::Type::FLOAT32] = "DefaultF32";
        deserializerTypes[DataType::Type::FLOAT64] = "DefaultF64";
        deserializerTypes[DataType::Type::BOOLEAN] = "DefaultBOOL";
        /// The raw JSON text of a string is not its value, it still carries the JSON escape sequences. Only the JSON deserializers
        /// decode them, the default ones would hand the escape sequences through to the record verbatim.
        deserializerTypes[DataType::Type::CHAR] = "JSONCHAR";
        deserializerTypes[DataType::Type::VARSIZED] = "JSONVARSIZED";

        /// Override the datatype defaults for the fields that the user configured a deserializer for
        fieldDeserializerTypes = parseValueDeserializerOverrides(deserializerOverrides, this->fieldNamesOutput);
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<SIMDJSONInputFormatIndexer> create(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
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
            Private{},
            config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER),
            std::move(jsonPointersToFields),
            std::move(fieldNamesOutput),
            std::move(fieldDataTypes),
            config.getFromConfig(InputFormatterDescriptor::VALUE_DESERIALIZERS));
    }

    ~SIMDJSONInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, 1}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return ""; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

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
