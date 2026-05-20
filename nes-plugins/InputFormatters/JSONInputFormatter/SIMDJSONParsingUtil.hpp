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
#include <string>
#include <string_view>
#include <type_traits>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDJSONInputFormatIndexer.hpp>
#include <SIMDJSONRawBufferIndex.hpp>
#include <function.hpp>
#include <nameof.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

namespace NES
{

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
parseJsonFixedSizeIntoVarValProxy(FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer);

inline simdjson::simdjson_result<simdjson::ondemand::value> accessSIMDJsonFieldOrThrow(
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

inline bool checkIsNullJsonProxy(
    const FieldIndex fieldIndex, const SIMDJSONRawBufferIndex* simdJsonRawBufferIndex, const SIMDJSONInputFormatIndexer* simdIndexer)
{
    const auto fieldNameStr = simdIndexer->getFieldNameInJsonAt(fieldIndex).asCanonicalString();
    const std::string_view fieldName = fieldNameStr;
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();

    /// First, we check if the key is not in the doc. If this is the case, we can return true, as this counts as null
    if (not currentDoc[fieldName].has_value())
    {
        return true;
    }

    /// Second, we need to check if the key is equal to one of the null values
    if (accessSIMDJsonFieldOrThrow(currentDoc, fieldName).is_null())
    {
        return true;
    }
    return false;
}

VarVal parseJsonVarSized(
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer,
    bool nullable);


void writeValueToRecord(
    DataType dataType,
    Record& record,
    const QualifiedIdentifier& fieldName,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer);

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

template <typename T>
[[nodiscard]] VarVal parseJsonFixedSizeIntoVarVal(
    const bool nullable,
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer)
{
    if (nullable)
    {
        const auto parseResult = nautilus::invoke(
            {nautilus::ModRefInfo::Ref}, parseJsonFixedSizeIntoVarValProxy<T, true>, fieldIndex, rawBufferIndex, indexer);
        const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResultFixed<T>, value));
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(parseResult, offsetof(ParseResultFixed<T>, isNull));
        return VarVal{nautilusValue, nullable, isNull};
    }
    const auto parseResult
        = nautilus::invoke({nautilus::ModRefInfo::Ref}, parseJsonFixedSizeIntoVarValProxy<T, false>, fieldIndex, rawBufferIndex, indexer);
    const nautilus::val<T> nautilusValue = *getMemberWithOffset<T>(parseResult, offsetof(ParseResultFixed<T>, value));
    return VarVal{nautilusValue, nullable, false};
}

/// (Proxy) functions being called via nautilus::invoke() can not be member functions. Thus, we need to implement them outside of the class
template <typename T, bool Nullable>
requires(
    not(std::is_same_v<T, int8_t*> || std::is_same_v<T, uint8_t*> || std::is_same_v<T, std::byte*> || std::is_same_v<T, char*>
        || std::is_same_v<T, unsigned char*> || std::is_same_v<T, signed char*>))
ParseResultFixed<T>*
parseJsonFixedSizeIntoVarValProxy(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
{
    PRECONDITION(dynamic_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex) != nullptr, "rawBufferIndex must be a SIMDJSONRawBufferIndex");
    PRECONDITION(dynamic_cast<const SIMDJSONInputFormatIndexer*>(indexer) != nullptr, "indexer must be a SIMDJSONInputFormatIndexer");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    auto* simdJsonRawBufferIndex = static_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    const auto* simdIndexer = static_cast<const SIMDJSONInputFormatIndexer*>(indexer);
    PRECONDITION(
        fieldIndex < simdIndexer->getNumberOfFields(),
        "fieldIndex {} is out of bounds for schema keys of size: {}",
        fieldIndex,
        simdIndexer->getNumberOfFields());

    auto returnNullValueOrParsed = [](std::optional<T> parsedValue, ParseResultFixed<T>& result) -> ParseResultFixed<T>*
    {
        if (not parsedValue.has_value())
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
        result.value = static_cast<T>(parsedValue.value());
        return &result;
    };

    /// We use the thread local to return multiple values.
    /// C++ guarantees that the returned address is valid throughout the lifetime of this thread.
    thread_local static ParseResultFixed<T> result;
    result.isNull = false;

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, simdJsonRawBufferIndex, simdIndexer))
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
    }

    const auto fieldNameStr = simdIndexer->getFieldNameInJsonAt(fieldIndex).asCanonicalString();
    const std::string_view fieldName = fieldNameStr;
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();
    auto simdJsonResult = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);
    /// Order is important, since signed_integral<char> is true and unsigned_integral<bool> is true
    if constexpr (std::same_as<T, bool>)
    {
        const auto parsedValue = parseSIMDJsonValueOrThrow<T, Nullable>(simdJsonResult.get_bool(), simdJsonResult, "bool", fieldName);
        return returnNullValueOrParsed(parsedValue, result);
    }
    else if constexpr (std::same_as<T, char>)
    {
        const auto parsedValue
            = parseSIMDJsonValueOrThrow<std::string_view, Nullable>(simdJsonResult.get_string(), simdJsonResult, "char", fieldName);
        if (not parsedValue.has_value())
        {
            result.isNull = true;
            result.value = T{0};
            return &result;
        }
        PRECONDITION(parsedValue.value().size() == 1, "Cannot take {} as character, because size is not 1", parsedValue.value());
        result.value = static_cast<T>(parsedValue.value()[0]);
        return &result;
    }
    else if constexpr (std::signed_integral<T>)
    {
        const auto parsedValue
            = parseSIMDJsonValueOrThrow<int64_t, Nullable>(simdJsonResult.get_int64(), simdJsonResult, "integer", fieldName);
        return returnNullValueOrParsed(parsedValue, result);
    }
    else if constexpr (std::unsigned_integral<T>)
    {
        const auto parsedValue
            = parseSIMDJsonValueOrThrow<uint64_t, Nullable>(simdJsonResult.get_uint64(), simdJsonResult, "unsigned", fieldName);
        return returnNullValueOrParsed(parsedValue, result);
    }
    else if constexpr (std::is_same_v<T, double>)
    {
        const auto parsedValue = parseSIMDJsonValueOrThrow<T, Nullable>(simdJsonResult.get_double(), simdJsonResult, "double", fieldName);
        return returnNullValueOrParsed(parsedValue, result);
    }
    else if constexpr (std::is_same_v<T, float>)
    {
        const auto parsedValue
            = parseSIMDJsonValueOrThrow<double, Nullable>(simdJsonResult.get_double(), simdJsonResult, "float", fieldName);
        return returnNullValueOrParsed(parsedValue, result);
    }
    else
    {
        throw UnknownDataType("Can not parse {}", NAMEOF_TYPE(T));
    }
}

template <bool Nullable>
ParsedResultVariableSized* parseJsonVarSizedProxy(FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
{
    PRECONDITION(dynamic_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex) != nullptr, "rawBufferIndex must be a SIMDJSONRawBufferIndex");
    PRECONDITION(dynamic_cast<const SIMDJSONInputFormatIndexer*>(indexer) != nullptr, "indexer must be a SIMDJSONInputFormatIndexer");
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    auto* simdJsonRawBufferIndex = static_cast<SIMDJSONRawBufferIndex*>(rawBufferIndex);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast): type verified by PRECONDITION above.
    const auto* simdIndexer = static_cast<const SIMDJSONInputFormatIndexer*>(indexer);
    PRECONDITION(
        fieldIndex < simdIndexer->getNumberOfFields(),
        "fieldIndex {} is out of bounds for schema keys of size: {}",
        fieldIndex,
        simdIndexer->getNumberOfFields());

    /// We use thread_local to ensure that result exists as long as the thread exists.
    /// We require this, as we return a pointer to the storage, due to us not being able to return two values in nautilus, i.e.,
    /// the size of the var sized and the pointer to it
    thread_local ParsedResultVariableSized result{};

    /// Checking if the field is null but only if the field is nullable
    if constexpr (Nullable)
    {
        if (checkIsNullJsonProxy(fieldIndex, simdJsonRawBufferIndex, simdIndexer))
        {
            constexpr auto sizeOfValue = 0;
            result = ParsedResultVariableSized{.varSizedPointer = nullptr, .size = sizeOfValue, .isNull = true};
            return &result;
        }
    }
    auto currentDoc = *simdJsonRawBufferIndex->getDocStreamIterator();
    const auto fieldNameStr = simdIndexer->getFieldNameInJsonAt(fieldIndex).asCanonicalString();
    const std::string_view fieldName = fieldNameStr;

    /// Get the value from the document and convert it to a span of bytes
    const std::string_view value = accessSIMDJsonFieldOrThrow(currentDoc, fieldName);

    result = ParsedResultVariableSized{.varSizedPointer = value.data(), .size = value.size(), .isNull = false};
    return &result;
}

inline VarVal parseJsonVarSized(
    const nautilus::val<FieldIndex>& fieldIndex,
    const nautilus::val<RawBufferIndex*>& rawBufferIndex,
    const nautilus::val<const InputFormatIndexer*>& indexer,
    const bool nullable)
{
    if (nullable)
    {
        const auto varSizedResult = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref}, parseJsonVarSizedProxy<true>, fieldIndex, rawBufferIndex, indexer);
        const VariableSizedData varSizedString{
            *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(ParsedResultVariableSized, varSizedPointer)),
            *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(ParsedResultVariableSized, size))};
        const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(varSizedResult, offsetof(ParsedResultVariableSized, isNull));
        return VarVal{VariableSizedData{varSizedString}, nullable, isNull};
    }

    const auto varSizedResult
        = nautilus::invoke({.modRefInfo = nautilus::ModRefInfo::Ref}, parseJsonVarSizedProxy<false>, fieldIndex, rawBufferIndex, indexer);
    const VariableSizedData varSizedString{
        *getMemberWithOffset<int8_t*>(varSizedResult, offsetof(ParsedResultVariableSized, varSizedPointer)),
        *getMemberWithOffset<uint64_t>(varSizedResult, offsetof(ParsedResultVariableSized, size))};
    return VarVal{VariableSizedData{varSizedString}, nullable, false};
}

}
