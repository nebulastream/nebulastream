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
#include <ranges>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
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
struct ParseResultFixed
{
    T value;
    bool isNull;
};

struct ParsedResultVariableSized
{
    const char* varSizedPointer;
    uint64_t size;
    bool isNull;
};

template <typename T, bool Nullable>
requires(
    not(std::is_same_v<T, int8_t*> || std::is_same_v<T, uint8_t*> || std::is_same_v<T, std::byte*> || std::is_same_v<T, char*>
        || std::is_same_v<T, unsigned char*> || std::is_same_v<T, signed char*>))
ParseResultFixed<T>*
parseJsonFixedSizeIntoVarValProxy(FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData);

bool checkIsNullJsonProxy(simdjson::simdjson_result<simdjson::ondemand::value>& simdJsonResult) noexcept;

struct SIMDJSONMetaData
{
    explicit SIMDJSONMetaData(const ParserConfig& config, const TupleBufferRef& tupleBufferRef)
        : fieldNamesOutput(
              tupleBufferRef.getAllFieldNames() | std::views::transform([](const auto& idList) { return *std::ranges::rbegin(idList); })
              | std::ranges::to<std::vector>())
        , fieldDataTypes(tupleBufferRef.getAllDataTypes())
        , tupleDelimiter(config.tupleDelimiter)

    {
        PRECONDITION(
            config.fieldDelimiter.size() == 1,
            "Delimiters must be of size '1 byte', but the field delimiter was {} (size {})",
            config.fieldDelimiter,
            config.fieldDelimiter.size());

        /// Format each identifier for JSON field lookup:
        /// - Unquoted (case-insensitive) identifiers are uppercased to match JSON keys
        /// - Quoted (case-sensitive) identifiers have their quotes stripped, preserving case
        for (const auto& fieldName : tupleBufferRef.getAllFieldNames())
        {
            fieldNamesInJson.emplace_back(fmt::format("{}", *std::ranges::rbegin(fieldName)));
        }

        PRECONDITION(fieldNamesInJson.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        PRECONDITION(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
    };

    std::string_view getTupleDelimitingBytes() const { return this->tupleDelimiter; }

    static QuotationType getQuotationType() { return QuotationType::DOUBLE_QUOTE; }

    [[nodiscard]] const Identifier& getFieldNameAt(const nautilus::static_val<uint64_t>& i) const { return fieldNamesOutput[i]; }

    [[nodiscard]] const std::string& getFieldNameInJsonAt(const nautilus::static_val<uint64_t>& i) const { return fieldNamesInJson[i]; }

    [[nodiscard]] const DataType& getFieldDataTypeAt(const nautilus::static_val<uint64_t>& i) const { return fieldDataTypes[i]; }

    [[nodiscard]] static const std::vector<std::string>& getNullValues()
    {
        INVARIANT(false, "This method should not be called, as SIMDJson has a is_null() method");
        std::unreachable();
    }

    [[nodiscard]] uint64_t getNumberOfFields() const
    {
        INVARIANT(fieldNamesOutput.size() == fieldDataTypes.size(), "No. fields must be equal to no. data types");
        return fieldNamesOutput.size();
    }

private:
    std::vector<std::string> fieldNamesInJson;
    std::vector<Identifier> fieldNamesOutput;
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
    [[nodiscard]] VarVal parseJsonFixedSizeIntoVarVal(
        const bool nullable,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData) const
    {
        if (nullable)
        {
            const auto parseResult = nautilus::invoke(
                {nautilus::ModRefInfo::Ref}, parseJsonFixedSizeIntoVarValProxy<T, true>, fieldIndex, fieldIndexFunction, metaData);
            const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResultFixed<T>, value));
            const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResultFixed<T>, isNull));
            return VarVal{nautilusValue, nullable, isNull};
        }
        const auto parseResult = nautilus::invoke(
            {nautilus::ModRefInfo::Ref}, parseJsonFixedSizeIntoVarValProxy<T, false>, fieldIndex, fieldIndexFunction, metaData);
        const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResultFixed<T>, value));
        return VarVal{nautilusValue, nullable, false};
    }

    static VarVal parseJsonVarSized(
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<SIMDJSONMetaData*>& metaData,
        bool nullable);

    void writeValueToRecord(
        DataType dataType,
        Record& record,
        const IdentifierList& fieldName,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<SIMDJSONFIF*>& fieldIndexFunction,
        const nautilus::val<const SIMDJSONMetaData*>& metaData) const;

    template <typename IndexerMetaData>
    [[nodiscard]] Record applyReadSpanningRecord(
        const std::vector<Record::RecordFieldIdentifier>& projections,
        const nautilus::val<int8_t*>&,
        const nautilus::val<uint64_t>&,
        const IndexerMetaData& metaData,
        nautilus::val<SIMDJSONFIF*> fieldIndexFunction) const
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
                fieldDataType, record, fieldName, fieldIndex, fieldIndexFunction, nautilus::val<const IndexerMetaData*>(&metaData));
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

    static simdjson::simdjson_result<simdjson::ondemand::value>
    accessSIMDJsonFieldOrThrow(simdjson::simdjson_result<simdjson::ondemand::value> simdJsonResult, const std::string_view fieldName)
    {
        if (not simdJsonResult.has_value())
        {
            throw FieldNotFound(
                "SimdJson has not found the fieldName {} with error: {}", fieldName, magic_enum::enum_name(simdJsonResult.error()));
        }
        return simdJsonResult;
    }

    /// Navigates to a field using find_field_unordered, splitting the field path
    /// (e.g., "KEY" or "EXTRA_KEY/NAME") on '/' and navigating step by step.
    /// Unlike at_pointer, find_field_unordered does NOT call rewind(), so the parser's
    /// internal string buffer is preserved across calls, allowing VARSIZED string_views
    /// to remain valid until the entire tuple has been processed.
    static simdjson::simdjson_result<simdjson::ondemand::value>
    navigateToField(simdjson::simdjson_result<simdjson::ondemand::document_reference>& doc, const std::string_view fieldPath)
    {
        auto slashPos = fieldPath.find('/');
        if (slashPos == std::string_view::npos)
        {
            /// Simple (non-nested) field
            return doc.find_field_unordered(fieldPath);
        }

        /// Nested field: navigate step by step through each path segment
        auto result = doc.find_field_unordered(fieldPath.substr(0, slashPos));
        auto path = fieldPath.substr(slashPos + 1);

        for (slashPos = path.find('/'); slashPos != std::string_view::npos; slashPos = path.find('/'))
        {
            result = result.find_field_unordered(path.substr(0, slashPos));
            path = path.substr(slashPos + 1);
        }
        return result.find_field_unordered(path);
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

/// (Proxy) functions being called via nautius::invoke() can not be member functions. Thus, we need to implement them outside the class
template <typename T, bool Nullable>
requires(
    not(std::is_same_v<T, int8_t*> || std::is_same_v<T, uint8_t*> || std::is_same_v<T, std::byte*> || std::is_same_v<T, char*>
        || std::is_same_v<T, unsigned char*> || std::is_same_v<T, signed char*>))
ParseResultFixed<T>*
parseJsonFixedSizeIntoVarValProxy(const FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, const SIMDJSONMetaData* metaData)
{
    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static ParseResultFixed<T> result;
    result.isNull = false;

    /// Navigate to the field once and reuse the result for both the null check and value extraction.
    const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);
    auto currentDoc = *fieldIndexFunction->getDocStreamIterator();

    if constexpr (Nullable)
    {
        auto simdJsonResult = SIMDJSONFIF::navigateToField(currentDoc, fieldName);
        if (checkIsNullJsonProxy(simdJsonResult))
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
    }

    auto simdJsonResult = SIMDJSONFIF::accessSIMDJsonFieldOrThrow(SIMDJSONFIF::navigateToField(currentDoc, fieldName), fieldName);
    /// Order is important, since signed_integral<char> is true and unsigned_integral<bool> is true
    if constexpr (std::same_as<T, bool>)
    {
        result.value = static_cast<T>(SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_bool(), simdJsonResult, "bool", fieldName));
        return &result;
    }
    else if constexpr (std::same_as<T, char>)
    {
        const auto strValue = SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_string(), simdJsonResult, "char", fieldName);
        PRECONDITION(strValue.size() == 1, "Cannot take {} as character, because size is not 1", strValue);
        result.value = static_cast<T>(strValue[0]);
        return &result;
    }
    else if constexpr (std::signed_integral<T>)
    {
        result.value
            = static_cast<T>(SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_int64(), simdJsonResult, "integer", fieldName));
        return &result;
    }
    else if constexpr (std::unsigned_integral<T>)
    {
        result.value
            = static_cast<T>(SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_uint64(), simdJsonResult, "unsigned", fieldName));
        return &result;
    }
    else if constexpr (std::is_same_v<T, double>)
    {
        result.value
            = static_cast<T>(SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_double(), simdJsonResult, "double", fieldName));
        return &result;
    }
    else if constexpr (std::is_same_v<T, float>)
    {
        result.value
            = static_cast<T>(SIMDJSONFIF::parseSIMDJsonValueOrThrow(simdJsonResult.get_double(), simdJsonResult, "float", fieldName));
        return &result;
    }
    else
    {
        throw UnknownDataType("Can not parse {}", NAMEOF_TYPE(T));
    }
}

template <bool Nullable>
ParsedResultVariableSized* parseJsonVarSizedProxy(FieldIndex fieldIndex, SIMDJSONFIF* fieldIndexFunction, SIMDJSONMetaData* metaData)
{
    /// We use thread_local to ensure that result exists as long as the thread exist.
    /// We require this, as we return a pointer to the storage, due to use not being able to return two values in nautilus, i.e.,
    /// the size of the var sized and the pointer to it
    thread_local static ParsedResultVariableSized result{};

    INVARIANT(
        fieldIndex < metaData->getNumberOfFields(),
        "fieldIndex {} is out or bounds for schema keys of size: {}",
        fieldIndex,
        metaData->getNumberOfFields());

    /// Navigate to the field once and reuse the result for both the null check and value extraction.
    auto currentDoc = *fieldIndexFunction->getDocStreamIterator();
    const auto& fieldName = metaData->getFieldNameInJsonAt(fieldIndex);

    if constexpr (Nullable)
    {
        auto simdJsonResult = SIMDJSONFIF::navigateToField(currentDoc, fieldName);
        if (checkIsNullJsonProxy(simdJsonResult))
        {
            result = ParsedResultVariableSized{.varSizedPointer = nullptr, .size = 0, .isNull = true};
            return &result;
        }
    }


    /// Get the value from the document and convert it to a span of bytes
    auto simdJsonResult = SIMDJSONFIF::accessSIMDJsonFieldOrThrow(SIMDJSONFIF::navigateToField(currentDoc, fieldName), fieldName);
    const std::string_view value = simdJsonResult.get_string().value();
    result = ParsedResultVariableSized{.varSizedPointer = value.data(), .size = value.size(), .isNull = false};
    return &result;
}

}
