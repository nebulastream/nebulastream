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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <FieldIndexFunction.hpp>
#include <RawTupleBuffer.hpp>
#include <RawValueParser.hpp>
#include <nameof.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{
class SIMDJSONFIF;
struct SIMDJSONMetaData;

template <typename T>
struct RawResultFixed
{
    T nullValue;
    bool isNull;
    const int8_t* ptrToRawJson;
    uint64_t sizeOfRawJson;
};

struct ParsedResultVariableSized
{
    const char* varSizedPointer;
    uint64_t size;
    bool isNull;
};

template <typename T, bool Nullable>
RawResultFixed<T>* getRawJsonWithNullCheck(FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData);

bool checkIsNullJsonProxy(FieldIndex fieldIndex, const SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData) noexcept;

struct ConfigParametersSIMDJSON
{
    static inline const DescriptorConfig::ConfigParameter<std::string> TUPLE_DELIMITER{
        "tuple_delimiter",
        "\n",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TUPLE_DELIMITER, config); }};

    static inline const std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(InputFormatterDescriptor::parameterMap, TUPLE_DELIMITER);
};

struct SIMDJSONMetaData
{
    explicit SIMDJSONMetaData(const InputFormatterDescriptor& config, const TupleBufferRef& tupleBufferRef)
        : fieldNamesOutput(tupleBufferRef.getAllFieldNames())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , tupleDelimiter(config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER))

    {
        PRECONDITION(
            config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER).size() == 1,
            "Delimiters must be of size '1 byte', but the field delimiter was {} (size {})",
            config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER),
            config.getFromConfig(ConfigParametersSIMDJSON::TUPLE_DELIMITER).size());

        /// We expect the names in the json file to not be source qualified
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            if (const auto& qualifierPosition = fieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR); qualifierPosition != std::string::npos)
            {
                fieldNamesInJson.emplace_back(fieldName.substr(qualifierPosition + 1));
            }
            else
            {
                fieldNamesInJson.emplace_back(fieldName);
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

    [[nodiscard]] static const std::vector<std::string>& getNullValues()
    {
        static std::vector<std::string> nullValues{""};
        return nullValues;
    }

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
    void parseJsonFixedSizeIntoVarVal(
        const DataType dataType,
        Record& record,
        const std::string& fieldName,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData,
        const std::string& parserType) const
    {
        if (dataType.nullable)
        {
            /// We need to do a nullcheck
            /// We use the SIMDJSON interface for this and return early, if the value is null
            const auto parseResult
                = nautilus::invoke({nautilus::ModRefInfo::Ref}, getRawJsonWithNullCheck<T, true>, fieldIndex, fieldIndexFunction, metaData);
            const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(RawResultFixed<T>, nullValue));
            const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(RawResultFixed<T>, isNull));
            if (isNull)
            {
                record.write(fieldName, VarVal{nautilusValue, nautilus::val<bool>{true}, isNull});
            }
            else
            {
                /// Get the ptr and size of the raw value and let the raw value parser handle the rest
                const nautilus::val<int8_t*> rawPtr = *getMemberWithOffset<int8_t*>(parseResult, offsetof(RawResultFixed<T>, ptrToRawJson));
                const nautilus::val<uint64_t> size
                    = *getMemberWithOffset<uint64_t>(parseResult, offsetof(RawResultFixed<T>, sizeOfRawJson));
                parseRawValueIntoRecord(dataType, record, rawPtr, size, fieldName, SIMDJSONMetaData::getNullValues(), QuotationType::DOUBLE_QUOTE, parserType);
            }
            return;
        }
        const auto parseResult
            = nautilus::invoke({nautilus::ModRefInfo::Ref}, getRawJsonWithNullCheck<T, false>, fieldIndex, fieldIndexFunction, metaData);
        const nautilus::val<int8_t*> rawPtr = *getMemberWithOffset<int8_t*>(parseResult, offsetof(RawResultFixed<T>, ptrToRawJson));
        const nautilus::val<uint64_t> size = *getMemberWithOffset<uint64_t>(parseResult, offsetof(RawResultFixed<T>, sizeOfRawJson));
        parseRawValueIntoRecord(dataType, record, rawPtr, size, fieldName, SIMDJSONMetaData::getNullValues(), QuotationType::DOUBLE_QUOTE, parserType);
    }

    static VarVal parseJsonVarSized(
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<SIMDJSONMetaData*>& metaData,
        bool nullable);

    void writeValueToRecord(
        DataType dataType,
        Record& record,
        const std::string& fieldName,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData,
        const std::string& parserType) const;

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>&,
        const nautilus::val<uint64_t>&,
        const IndexerMetaData& metaData,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction,
        const std::unordered_map<DataType::Type, std::string>& parserTypes) const
    {
        Record record;
        for (nautilus::static_val<FieldIndex> i = 0; i < static_cast<FieldIndex>(metaData.getNumberOfFields()); ++i)
        {
            const auto& fieldName = metaData.getFieldNameAt(i);

            if (std::ranges::find(projections, fieldName) == projections.end())
            {
                continue;
            }

            auto fieldIndex = static_cast<nautilus::val<FieldIndex>>(i);
            const auto& fieldDataType = metaData.getFieldDataTypeAt(i);
            writeValueToRecord(
                fieldDataType,
                record,
                fieldName,
                fieldIndex,
                fieldIndexFunction,
                nautilus::val<const IndexerMetaData*>(&metaData),
                parserTypes.at(fieldDataType.type));
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

    /// Tries to parse the value. If it can not parse the value and the field is nullable, we return nullopt to signal
    /// to the callee that the value shall be NULL. If it can not parse the value and the field is NOT nullable, we throw.
    template <typename T, bool Nullable>
    static std::optional<T> parseSIMDJsonValueOrThrow(
        simdjson::simdjson_result<T> simdJsonValue,
        simdjson::simdjson_result<simdjson::ondemand::value>& rawVal,
        const std::string_view expectedType,
        const std::string_view expectedField)
    {
        if (not simdJsonValue.has_value())
        {
            if constexpr (Nullable)
            {
                return {};
            }
            throw CannotFormatMalformedStringValue(
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
        const auto simdJsonResult = simdJsonReference[fieldName];
        if (not simdJsonResult.has_value())
        {
            throw FieldNotFound(
                "SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(simdJsonResult.error()));
        }
        return simdJsonResult;
    }

    [[nodiscard]] simdjson::ondemand::document_stream::iterator getDocStreamIterator() const { return docStreamIterator; }

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

/// Get a raw pointer and the size of the current field. If required, perform a null check.
/// If the null check succeeds, a null value of the provided type T will be returned instead
template <typename T, bool Nullable>
RawResultFixed<T>* getRawJsonWithNullCheck(const FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData)
{
    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static RawResultFixed<T> result;
    result.isNull = false;

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, fieldIndexFunction, metaData))
        {
            result.isNull = true;
            result.nullValue = T{0};
            result.ptrToRawJson = nullptr;
            result.sizeOfRawJson = 0;
            return &result;
        }
        result.isNull = false;
        result.nullValue = T{0};
    }

    const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);
    auto currentDoc = *fieldIndexFunction->getDocStreamIterator();
    auto simdJsonResult = SIMDJSONFIF::accessSIMDJsonFieldOrThrow(currentDoc, fieldName);
    /// Get the raw string view over the SIMD json value and return a ptr to it
    const auto rawSimdJsonResult = simdJsonResult.raw_json().value();
    result.ptrToRawJson = reinterpret_cast<const int8_t*>(rawSimdJsonResult.data());
    result.sizeOfRawJson = rawSimdJsonResult.size();
    return &result;
}

template <bool Nullable>
ParsedResultVariableSized* parseJsonVarSizedProxy(FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, SIMDJSONMetaData* metaData)
{
    /// We use thread_local to ensure that result exists as long as the thread exist.
    /// We require this, as we return a pointer to the storage, due to use not being able to return two values in nautilus, i.e.,
    /// the size of the var sized and the pointer to it
    thread_local static ParsedResultVariableSized result{};

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, fieldIndexFunction, metaData))
        {
            constexpr auto sizeOfValue = 0;
            result = ParsedResultVariableSized{.varSizedPointer = nullptr, .size = sizeOfValue, .isNull = true};
            return &result;
        }
    }

    INVARIANT(
        fieldIndex < metaData->getNumberOfFields(),
        "fieldIndex {} is out or bounds for schema keys of size: {}",
        fieldIndex,
        metaData->getNumberOfFields());
    auto currentDoc = *fieldIndexFunction->getDocStreamIterator();
    const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);

    /// Get the value from the document and convert it to a span of bytes
    const std::string_view value = SIMDJSONFIF::accessSIMDJsonFieldOrThrow(currentDoc, fieldName);

    result = ParsedResultVariableSized{.varSizedPointer = value.data(), .size = value.size(), .isNull = false};
    return &result;
}

}
