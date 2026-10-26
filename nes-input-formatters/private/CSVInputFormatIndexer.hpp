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
#include <expected>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <ValueDeserializerUtil.hpp>
#include <static.hpp>

namespace NES
{

/// Formatter-defined config struct: instantiated from the generic config by the
/// InputFormatterConfig registry entry, carried through the InputFormatterDescriptor as std::any,
/// and serialized via reflection of exactly this struct (all members are reflectable).
struct CSVInputFormatterConfig
{
    bool allowCommasInStrings;
    char tupleDelimiter;
    char fieldDelimiter;
    std::string valueDeserializers;

    static std::expected<CSVInputFormatterConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class CSVInputFormatIndexer : public InputFormatIndexer
{
    /// Passkey idiom (to enforce checks before calling the constructor)
    struct Private
    {
        explicit Private() = default;
    };

public:
    static constexpr std::string_view NAME = "CSV";
    static constexpr size_t SIZE_OF_TUPLE_DELIMITER = 1;
    static constexpr size_t SIZE_OF_FIELD_DELIMITER = 1;

    explicit CSVInputFormatIndexer(
        Private,
        const char tupleDelimiter,
        const char fieldDelimiter,
        const bool allowCommasInStrings,
        const std::vector<Record::RecordFieldIdentifier>& fieldNames,
        const std::string& deserializerOverrides)
        : tupleDelimiter(tupleDelimiter)
        , fieldDelimiter(fieldDelimiter)
        , allowCommasInStrings(allowCommasInStrings)
        , numberOfFields(fieldNames.size())
        , nullValues({""})
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
        deserializerTypes[DataType::Type::CHAR] = "DefaultCHAR";
        deserializerTypes[DataType::Type::VARSIZED] = "DefaultVARSIZED";

        /// Override the datatype defaults for the fields that the user configured a deserializer for
        fieldDeserializerTypes = parseValueDeserializerOverrides(deserializerOverrides, fieldNames);
    }

    /// Delegate constructor that applies preconditions before safely calling the constructor
    static std::unique_ptr<CSVInputFormatIndexer> create(const CSVInputFormatterConfig& config, const TupleBufferRef& tupleBufferRef)
    {
        return std::make_unique<CSVInputFormatIndexer>(
            Private{},
            config.tupleDelimiter,
            config.fieldDelimiter,
            config.allowCommasInStrings,
            tupleBufferRef.getAllFieldNames(),
            config.valueDeserializers);
    }

    ~CSVInputFormatIndexer() override = default;

    [[nodiscard]] std::unique_ptr<RawBufferIndex> indexRawBuffer(std::string_view rawBuffer) const override;

    [[nodiscard]] std::string_view getTupleDelimitingBytes() const override { return {&tupleDelimiter, SIZE_OF_TUPLE_DELIMITER}; }

    [[nodiscard]] std::string_view getFieldDelimitingBytes() const override { return {&fieldDelimiter, SIZE_OF_FIELD_DELIMITER}; }

    [[nodiscard]] const std::vector<std::string>& getNullValues() const override { return nullValues; }

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    char tupleDelimiter;
    char fieldDelimiter;
    bool allowCommasInStrings{};
    size_t numberOfFields;
    std::vector<std::string> nullValues;
};

}
