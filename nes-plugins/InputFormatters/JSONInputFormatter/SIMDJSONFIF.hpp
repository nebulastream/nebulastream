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

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <RawValueParser.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <Util/Strings.hpp>

namespace NES
{


struct SIMDJSONMetaData
{
    explicit SIMDJSONMetaData(const ParserConfig& config, const TupleBufferRef& tupleBufferRef)
        : fieldNamesOutput(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , tupleDelimiter(config.tupleDelimiter)

    {
        PRECONDITION(
            config.fieldDelimiter.size() == 1,
            "Delimiters must be of size '1 byte', but the field delimiter was {} (size {})",
            config.fieldDelimiter,
            config.fieldDelimiter.size());

        /// We expect the names in the json file to not be source qualified
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            if (const auto& qualifierPosition = fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR); qualifierPosition != std::string::npos)
            {

                const auto adaptedFiledName = replaceAll(fieldName.substr(qualifierPosition + 1), "$", "/");
                fieldNamesInJson.emplace_back(std::string("/").append(adaptedFiledName));
            }
            else
            {
                throw FormattingError(
                "Expected qualified field <source_name>${}, but only got {}",
                fieldName,
                fieldName);
            }
        }

        PRECONDITION(fieldNamesInJson.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        PRECONDITION(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
    };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNamesOutput[i];
    }

    [[nodiscard]] const Record::RecordFieldIdentifier& getFieldNameInJsonAt(const nautilus::static_val<uint64_t>& i) const
    {
        return fieldNamesInJson[i];
    }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return fieldDataTypes[i]; }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNamesOutput.size();
    }

private:
    std::vector<Record::RecordFieldIdentifier> fieldNamesInJson;
    std::vector<Record::RecordFieldIdentifier> fieldNamesOutput;
    std::vector<DataType> fieldDataTypes;
    std::string tupleDelimiter;
};

class SIMDJSONFIF final : public FieldIndexFunction<SIMDJSONFIF>
{
    friend FieldIndexFunction<SIMDJSONFIF>;

    /// Fieldstatic IndexFunction (CRTP) interface functions
    [[nodiscard]] FieldIndex applyGetByteOffsetOfFirstTuple() const { return this->offsetOfFirstTuple; }

    [[nodiscard]] FieldIndex applyGetByteOffsetOfLastTuple() const { return this->offsetOfLastTuple; }

    /// SIMDJSON can only determine the correct number of tuples after parsing the entire buffer
    [[nodiscard]] static size_t applyGetTotalNumberOfTuples() { return 0; }

    [[nodiscard]] static nautilus::val<bool>
    applyHasNext(const nautilus::val<uint64_t>&, const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction);

    template <typename T>
    static T parseSIMDJsonValueOrThrow(
        simdjson::simdjson_result<T> simdJsonValue,
        simdjson::simdjson_result<simdjson::ondemand::value>& rawVal,
        const std::string_view expectedType,
        const std::string_view expectedField)
    {
        if (not simdJsonValue.has_value())
        {
            throw FormattingError(
                "SimdJson could not parse field value {} of type '{}' belonging to field '{}' with error: {}",
                rawVal.raw_json().value(),
                expectedType,
                expectedField,
                magic_enum::enum_name(simdJsonValue.error()));
        }
        return simdJsonValue.value();
    }

    static simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
        simdjson::simdjson_result<simdjson::ondemand::document_reference>& simdJsonReference, const std::string_view fieldName)
    {
        const auto simdJsonResult = simdJsonReference.at_pointer(fieldName);
        if (not simdJsonResult.has_value())
        {
            throw FieldNotFound(
                "SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(simdJsonResult.error()));
        }
        return simdJsonResult;
    }

    template <typename T>
    nautilus::val<T> parseNonStringValueIntoNautilusRecord(
        nautilus::val<FieldIndex> fieldIdx,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        nautilus::val<const SIMDJSONMetaData*> metaData) const
    {
        return nautilus::invoke(
            +[](FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData)
            {
                const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);
                auto currentDoc = *fieldIndexFunction->docStreamIterator;
                auto simdJsonResult = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);
                /// Order is important, since signed_integral<char> is true and unsigned_integral<bool> is true
                if constexpr (std::same_as<T, bool>)
                {
                    const T value = parseSIMDJsonValueOrThrow(simdJsonResult.get_bool(), simdJsonResult, "bool", fieldName);
                    return value;
                }
                else if constexpr (std::same_as<T, char>)
                {
                    const std::string_view valueSV = currentDoc[fieldName];
                    PRECONDITION(valueSV.size() == 1, "Cannot take {} as character, because size is not 1", valueSV);
                    const T value = parseSIMDJsonValueOrThrow(simdJsonResult.get_string(), simdJsonResult, "char", fieldName)[0];
                    return value;
                }
                else if constexpr (std::signed_integral<T>)
                {
                    const auto value
                        = static_cast<T>(parseSIMDJsonValueOrThrow(simdJsonResult.get_int64(), simdJsonResult, "integer", fieldName));
                    return value;
                }
                else if constexpr (std::unsigned_integral<T>)
                {
                    const auto value
                        = static_cast<T>(parseSIMDJsonValueOrThrow(simdJsonResult.get_uint64(), simdJsonResult, "unsigned", fieldName));
                    return value;
                }
                else if constexpr (std::is_same_v<T, double>)
                {
                    const auto value
                        = static_cast<T>(parseSIMDJsonValueOrThrow(simdJsonResult.get_double(), simdJsonResult, "double", fieldName));
                    return value;
                }
                else if constexpr (std::is_same_v<T, float>)
                {
                    const auto value
                        = static_cast<T>(parseSIMDJsonValueOrThrow(simdJsonResult.get_double(), simdJsonResult, "float", fieldName));
                    return value;
                }
            },
            fieldIdx,
            fieldIndexFunction,
            metaData);
    }

    static VariableSizedData parseStringIntoNautilusRecord(
        const nautilus::val<FieldIndex>& fieldIdx,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<SIMDJSONMetaData*>& metaData,
        const ArenaRef& arenaRef);

    void writeValueToRecord(
        DataType::Type physicalType,
        Record& record,
        const std::string& fieldName,
        const nautilus::val<FieldIndex>& fieldIdx,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData,
        ArenaRef& arenaRef) const;

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>&,
        const nautilus::val<uint64_t>&,
        const IndexerMetaData& metaData,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        ArenaRef& arenaRef) const
    {
        Record record;
        for (nautilus::static_val<FieldIndex> i = 0; i < static_cast<FieldIndex>(metaData.getNumberOfFields()); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);

            if (std::ranges::find(projections, fieldName) == projections.end())
            {
                continue;
            }

            auto fieldIdx = static_cast<nautilus::val<FieldIndex>>(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            writeValueToRecord(
                fieldDataType.type,
                record,
                fieldName,
                fieldIdx,
                fieldIndexFunction,
                nautilus::val<const IndexerMetaData*>(&metaData),
                arenaRef);
        }
        /// Increment iterator and return record
        nautilus::invoke(
            +[](SIMDJSONFIF* simdJSONFIF)
            {
                ++simdJSONFIF->docStreamIterator;
                simdJSONFIF->isAtLastTuple = simdJSONFIF->docStreamIterator.at_end();
            },
            fieldIndexFunction);
        return record;
    }


public:
    SIMDJSONFIF() = default;
    ~SIMDJSONFIF() = default;

    /// Resets the indexes and pointers, calculates and sets the number of tuples in the current buffer, returns the total number of tuples.
    void markNoTupleDelimiters();

    void markWithTupleDelimiters(FieldIndex offsetToFirstTuple, std::optional<FieldIndex> offsetToLastTuple);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV);

    std::pair<bool, FieldIndex> indexJSON(std::string_view jsonSV, size_t batchSize);

private:
    bool isAtLastTuple{false};
    size_t numberOfFieldsInSchema{};
    FieldIndex offsetOfFirstTuple{};
    FieldIndex offsetOfLastTuple{};
    std::shared_ptr<simdjson::ondemand::parser> parser;
    std::shared_ptr<simdjson::ondemand::document_stream> docStream;
    simdjson::ondemand::document_stream::iterator docStreamIterator;
};

static_assert(std::is_standard_layout_v<SIMDJSONFIF>, "SIMDJSONFIF must have a standard layout");

}
