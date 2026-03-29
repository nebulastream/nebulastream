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
#include <utility>

#include <simdjson.h>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/FixedSizedData.hpp>
#include <Nautilus/DataTypes/StructData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <magic_enum/magic_enum.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexer.hpp>
#include <RawBufferIndex.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <common/FunctionAttributes.hpp>

/// Shared simdjson-driven JSON-to-Record parser.
///
/// One C++ entry point — `parseValue` — is the entire dispatch: a single
/// switch over `DataType::Type` either parses a primitive directly with
/// simdjson's typed accessor, wraps a string view, or, for FIXEDSIZED, loops
/// over the JSON array and recurses element-by-element into the same
/// function. The trace side issues one `nautilus::invoke` per top-level field
/// through `fieldProxy`. Primitives and strings land in a thread-local typed
/// slot; FIXEDSIZED arrays land in arena-allocated memory (arena allocation
/// happens trace-side, the buffer is handed in to `fieldProxy`).
namespace NES::JsonValueParser
{

/// Layout of the thread-local slot for a top-level primitive field. The
/// `isNull` byte sits at `offsetof(ParseResult<T>, isNull)`; for non-nullable
/// fields the parser writes only `value`, since `wrapPrimitive` reads
/// `isNull` only when nullable. Nested FIXEDSIZED elements use a packed `T[]`
/// layout (no per-element nullability), which is why the parser writes only
/// `value` for non-nullable cases.
template <typename T>
struct ParseResult
{
    T value;
    bool isNull;
};

struct ParseResultVarSized
{
    const char* ptr;
    uint64_t size;
    bool isNull;
};

inline bool isNullOrMissing(simdjson::simdjson_result<simdjson::ondemand::value>& jsonValue)
{
    return not jsonValue.has_value() || jsonValue.is_null();
}

[[noreturn]] inline void throwFieldNotFound(const std::string_view fieldName)
{
    throw FieldNotFound(
        "Required field '{}' is missing or null in the JSON document; declare the field as nullable or fix the input.", fieldName);
}

/// Inline byte size of a single field for STRUCT layouts. Mirrors the
/// equivalent helpers in `DataType.cpp` and `StructData.cpp`; the three must
/// stay in lockstep or the parser will write bytes the readers can't decode.
inline size_t inlineFieldSizeInBytes(const DataType& field)
{
    if (field.type == DataType::Type::FIXEDSIZED)
    {
        const auto elementSize = DataType{field.elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
        return static_cast<size_t>(field.count) * elementSize;
    }
    if (field.type == DataType::Type::STRUCT)
    {
        size_t total = 0;
        for (const auto& [name, sub] : field.fields)
        {
            total += inlineFieldSizeInBytes(sub);
        }
        return total;
    }
    return DataType{field.type, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
}

/// Single typed simdjson primitive accessor — used both for top-level fields
/// and for elements inside a FIXEDSIZED array. On a simdjson type-mismatch
/// either returns `nullopt` (nullable fields → null sentinel) or throws
/// (non-nullable fields → CannotFormatMalformedStringValue).
template <typename T>
inline std::optional<T>
extractPrimitive(simdjson::simdjson_result<simdjson::ondemand::value>& jsonValue, const std::string_view fieldName, const bool nullable)
{
    auto fail = [&](auto err, const std::string_view expectedType) -> std::optional<T>
    {
        if (nullable)
        {
            return std::nullopt;
        }
        throw CannotFormatMalformedStringValue(
            "Field '{}' could not be parsed as '{}': {}", fieldName, expectedType, magic_enum::enum_name(err));
    };
    /// Order matters: signed_integral<char> is true and unsigned_integral<bool> is true.
    if constexpr (std::same_as<T, bool>)
    {
        auto r = jsonValue.get_bool();
        return r.has_value() ? std::optional<T>{r.value()} : fail(r.error(), "bool");
    }
    else if constexpr (std::same_as<T, char>)
    {
        auto r = jsonValue.get_string();
        if (not r.has_value())
        {
            return fail(r.error(), "char");
        }
        PRECONDITION(r.value().size() == 1, "Cannot take {} as character, because size is not 1", r.value());
        return r.value()[0];
    }
    else if constexpr (std::signed_integral<T>)
    {
        auto r = jsonValue.get_int64();
        return r.has_value() ? std::optional<T>{static_cast<T>(r.value())} : fail(r.error(), "integer");
    }
    else if constexpr (std::unsigned_integral<T>)
    {
        auto r = jsonValue.get_uint64();
        return r.has_value() ? std::optional<T>{static_cast<T>(r.value())} : fail(r.error(), "unsigned integer");
    }
    else
    {
        static_assert(std::is_floating_point_v<T>);
        auto r = jsonValue.get_double();
        return r.has_value() ? std::optional<T>{static_cast<T>(r.value())} : fail(r.error(), "floating-point");
    }
}

namespace detail
{

template <typename T>
inline void writePrimitive(int8_t* output, const T value, const bool nullable, const bool isNull)
{
    *reinterpret_cast<T*>(output) = value;
    if (nullable)
    {
        *reinterpret_cast<bool*>(output + offsetof(ParseResult<T>, isNull)) = isNull;
    }
}

template <typename T>
inline void writePrimitiveFromJson(
    int8_t* output, simdjson::simdjson_result<simdjson::ondemand::value>& jsonValue, const std::string_view fieldName, const bool nullable)
{
    if (auto parsed = extractPrimitive<T>(jsonValue, fieldName, nullable); parsed.has_value())
    {
        writePrimitive<T>(output, parsed.value(), nullable, false);
    }
    else
    {
        writePrimitive<T>(output, T{}, true, true);
    }
}

inline void writeNullPrimitive(const DataType::Type type, int8_t* output)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            writePrimitive<bool>(output, false, true, true);
            return;
        case DataType::Type::CHAR:
            writePrimitive<char>(output, 0, true, true);
            return;
        case DataType::Type::INT8:
            writePrimitive<int8_t>(output, 0, true, true);
            return;
        case DataType::Type::INT16:
            writePrimitive<int16_t>(output, 0, true, true);
            return;
        case DataType::Type::INT32:
            writePrimitive<int32_t>(output, 0, true, true);
            return;
        case DataType::Type::INT64:
            writePrimitive<int64_t>(output, 0, true, true);
            return;
        case DataType::Type::UINT8:
            writePrimitive<uint8_t>(output, 0, true, true);
            return;
        case DataType::Type::UINT16:
            writePrimitive<uint16_t>(output, 0, true, true);
            return;
        case DataType::Type::UINT32:
            writePrimitive<uint32_t>(output, 0, true, true);
            return;
        case DataType::Type::UINT64:
            writePrimitive<uint64_t>(output, 0, true, true);
            return;
        case DataType::Type::FLOAT32:
            writePrimitive<float>(output, 0.0F, true, true);
            return;
        case DataType::Type::FLOAT64:
            writePrimitive<double>(output, 0.0, true, true);
            return;
        default:
            throw NotImplemented("Null sentinel for type {} is not supported.", magic_enum::enum_name(type));
    }
}

}

/// Single recursive entry point. Parses `jsonValue` according to `dataType`
/// and writes the result at `output`:
///
///   - Primitive (top-level): writes a `T` at offset 0, plus `isNull` at
///     `offsetof(ParseResult<T>, isNull)` iff nullable.
///   - VARSIZED:               writes a `ParseResultVarSized`.
///   - FIXEDSIZED:             iterates the JSON array and recurses into this
///                             same function with the (non-nullable) element
///                             type for each element, advancing `output` by
///                             `elementSize` per element.
///
/// FIXEDSIZED is the only case that needs storage beyond the trace-side
/// thread-local slot — the caller passes in arena-allocated memory.
inline void parseValue(
    const DataType& dataType,
    simdjson::simdjson_result<simdjson::ondemand::value>& jsonValue,
    const std::string_view fieldName,
    int8_t* output)
{
    if (isNullOrMissing(jsonValue))
    {
        if (dataType.nullable && dataType.type != DataType::Type::FIXEDSIZED)
        {
            if (dataType.type == DataType::Type::VARSIZED)
            {
                *reinterpret_cast<ParseResultVarSized*>(output) = {nullptr, 0, true};
                return;
            }
            detail::writeNullPrimitive(dataType.type, output);
            return;
        }
        throwFieldNotFound(fieldName);
    }

    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
            detail::writePrimitiveFromJson<bool>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::CHAR:
            detail::writePrimitiveFromJson<char>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::INT8:
            detail::writePrimitiveFromJson<int8_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::INT16:
            detail::writePrimitiveFromJson<int16_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::INT32:
            detail::writePrimitiveFromJson<int32_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::INT64:
            detail::writePrimitiveFromJson<int64_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::UINT8:
            detail::writePrimitiveFromJson<uint8_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::UINT16:
            detail::writePrimitiveFromJson<uint16_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::UINT32:
            detail::writePrimitiveFromJson<uint32_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::UINT64:
            detail::writePrimitiveFromJson<uint64_t>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::FLOAT32:
            detail::writePrimitiveFromJson<float>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::FLOAT64:
            detail::writePrimitiveFromJson<double>(output, jsonValue, fieldName, dataType.nullable);
            return;
        case DataType::Type::VARSIZED: {
            auto sv = jsonValue.get_string();
            if (not sv.has_value())
            {
                if (dataType.nullable)
                {
                    *reinterpret_cast<ParseResultVarSized*>(output) = {nullptr, 0, true};
                    return;
                }
                throw CannotFormatMalformedStringValue(
                    "Field '{}' could not be parsed as 'string': {}", fieldName, magic_enum::enum_name(sv.error()));
            }
            *reinterpret_cast<ParseResultVarSized*>(output) = {sv.value().data(), sv.value().size(), false};
            return;
        }
        case DataType::Type::FIXEDSIZED: {
            const DataType elementType{dataType.elementType, DataType::NULLABLE::NOT_NULLABLE};
            const auto elementSize = elementType.getSizeInBytesWithoutNull();
            auto array = jsonValue.get_array();
            if (not array.has_value())
            {
                throw CannotFormatMalformedStringValue(
                    "Field '{}': expected JSON array, simdjson reported error '{}'", fieldName, magic_enum::enum_name(array.error()));
            }
            uint64_t i = 0;
            for (auto element : array.value())
            {
                if (i >= dataType.count)
                {
                    throw CannotFormatMalformedStringValue(
                        "Field '{}': JSON array has more than the schema-declared {} elements", fieldName, dataType.count);
                }
                parseValue(elementType, element, fieldName, output + i * elementSize);
                ++i;
            }
            if (i != dataType.count)
            {
                throw CannotFormatMalformedStringValue(
                    "Field '{}': JSON array has {} elements but schema expects {}", fieldName, i, dataType.count);
            }
            return;
        }
        case DataType::Type::STRUCT: {
            auto object = jsonValue.get_object();
            if (not object.has_value())
            {
                throw CannotFormatMalformedStringValue(
                    "Field '{}': expected JSON object, simdjson reported error '{}'", fieldName, magic_enum::enum_name(object.error()));
            }
            int8_t* fieldOutput = output;
            for (const auto& [name, fieldType] : dataType.fields)
            {
                /// Inline layout — struct fields don't carry a per-field null byte. Strip
                /// nullability so the recursive call lays bytes out the way StructData
                /// expects when it later reads them back.
                const DataType nonNullableField = [&]
                {
                    DataType copy = fieldType;
                    copy.nullable = false;
                    return copy;
                }();
                auto subValue = object.find_field_unordered(name);
                if (isNullOrMissing(subValue))
                {
                    throwFieldNotFound(name);
                }
                parseValue(nonNullableField, subValue, name, fieldOutput);
                fieldOutput += inlineFieldSizeInBytes(fieldType);
            }
            return;
        }
        case DataType::Type::UNDEFINED:
            throw NotImplemented("Cannot parse undefined type.");
    }
    std::unreachable();
}

/// Per-formatter wrapper. The Traits struct supplies the concrete
/// RawBufferIndex / InputFormatIndexer types, the simdjson navigator, and
/// whether FIXEDSIZED is supported.
template <typename Traits>
struct JsonRecordParser
{
    /// Navigates to the field with simdjson and dispatches `parseValue` into
    /// `output`. Shared by all three per-kind proxies below.
    static void
    navigateAndParse(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer, int8_t* output)
    {
        auto* concreteRbi = static_cast<typename Traits::BufferIndex*>(rawBufferIndex);
        const auto* concreteIndexer = static_cast<const typename Traits::Indexer*>(indexer);
        const auto& dataType = concreteIndexer->getFieldDataTypeAt(fieldIndex);
        const auto& fieldName = concreteIndexer->getFieldNameInJsonAt(fieldIndex);

        auto currentDoc = *concreteRbi->getDocStreamIterator();
        auto navigated = Traits::navigate(currentDoc, fieldName);
        parseValue(dataType, navigated, fieldName, output);
    }

    /// nautilus::invoke target for a top-level primitive field. Writes the
    /// parsed value into a typed thread-local `ParseResult<T>` and returns
    /// its address.
    template <typename T>
    static ParseResult<T>* primitiveProxy(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
    {
        thread_local ParseResult<T> slot{};
        navigateAndParse(fieldIndex, rawBufferIndex, indexer, reinterpret_cast<int8_t*>(&slot));
        return &slot;
    }

    /// nautilus::invoke target for a top-level VARSIZED field. The simdjson
    /// string buffer outlives the per-record parse, so the returned pointer
    /// stays valid until the trace materializes the record.
    static ParseResultVarSized*
    varSizedProxy(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer)
    {
        thread_local ParseResultVarSized slot{};
        navigateAndParse(fieldIndex, rawBufferIndex, indexer, reinterpret_cast<int8_t*>(&slot));
        return &slot;
    }

    /// nautilus::invoke target for a FIXEDSIZED field. Writes elements
    /// straight into the arena-allocated buffer the trace handed in.
    static void
    fixedSizedProxy(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer, int8_t* output)
    {
        navigateAndParse(fieldIndex, rawBufferIndex, indexer, output);
    }

    /// Same shape as `fixedSizedProxy` — the trace pre-allocates a struct-sized
    /// arena buffer and the simdjson side parses field-by-field into it.
    static void structProxy(const FieldIndex fieldIndex, RawBufferIndex* rawBufferIndex, const InputFormatIndexer* indexer, int8_t* output)
    {
        navigateAndParse(fieldIndex, rawBufferIndex, indexer, output);
    }

    /// Trace-side dispatch. The actual JSON parsing all lives in
    /// `parseValue`; this switch only chooses how to wrap the bytes that the
    /// proxy wrote.
    static VarVal parseField(
        const DataType& dataType,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer,
        ArenaRef& arena)
    {
        switch (dataType.type)
        {
            case DataType::Type::BOOLEAN:
                return wrapPrimitive<bool>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::CHAR:
                return wrapPrimitive<char>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::INT8:
                return wrapPrimitive<int8_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::INT16:
                return wrapPrimitive<int16_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::INT32:
                return wrapPrimitive<int32_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::INT64:
                return wrapPrimitive<int64_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::UINT8:
                return wrapPrimitive<uint8_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::UINT16:
                return wrapPrimitive<uint16_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::UINT32:
                return wrapPrimitive<uint32_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::UINT64:
                return wrapPrimitive<uint64_t>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::FLOAT32:
                return wrapPrimitive<float>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::FLOAT64:
                return wrapPrimitive<double>(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::VARSIZED:
                return wrapVarSized(dataType.nullable, fieldIndex, rawBufferIndex, indexer);
            case DataType::Type::FIXEDSIZED:
                if constexpr (Traits::supportsFixedSized)
                {
                    return wrapFixedSized(dataType, fieldIndex, rawBufferIndex, indexer, arena);
                }
                else
                {
                    throw NotImplemented("This JSON formatter does not support FIXEDSIZED arrays; use the NestedJSON formatter instead.");
                }
            case DataType::Type::STRUCT:
                if constexpr (Traits::supportsFixedSized)
                {
                    return wrapStruct(dataType, fieldIndex, rawBufferIndex, indexer, arena);
                }
                else
                {
                    throw NotImplemented("This JSON formatter does not support STRUCT types; use the NestedJSON formatter instead.");
                }
            case DataType::Type::UNDEFINED:
                throw NotImplemented("Cannot parse undefined type.");
        }
        std::unreachable();
    }

    template <typename T>
    static VarVal wrapPrimitive(
        const bool nullable,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer)
    {
        const auto buffer = nautilus::invoke({nautilus::ModRefInfo::Ref}, primitiveProxy<T>, fieldIndex, rawBufferIndex, indexer);
        const nautilus::val<T> value = *getMemberWithOffset<T>(buffer, offsetof(ParseResult<T>, value));
        if (nullable)
        {
            const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(buffer, offsetof(ParseResult<T>, isNull));
            return VarVal{value, true, isNull};
        }
        return VarVal{value, false, false};
    }

    static VarVal wrapVarSized(
        const bool nullable,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer)
    {
        const auto buffer = nautilus::invoke({nautilus::ModRefInfo::Ref}, varSizedProxy, fieldIndex, rawBufferIndex, indexer);
        const VariableSizedData varSized{
            *getMemberWithOffset<int8_t*>(buffer, offsetof(ParseResultVarSized, ptr)),
            *getMemberWithOffset<uint64_t>(buffer, offsetof(ParseResultVarSized, size))};
        if (nullable)
        {
            const nautilus::val<bool> isNull = *getMemberWithOffset<bool>(buffer, offsetof(ParseResultVarSized, isNull));
            return VarVal{varSized, true, isNull};
        }
        return VarVal{varSized, false, false};
    }

    static VarVal wrapFixedSized(
        const DataType& dataType,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer,
        ArenaRef& arena)
    {
        const auto elementSize = DataType{dataType.elementType, DataType::NULLABLE::NOT_NULLABLE}.getSizeInBytesWithoutNull();
        const nautilus::val<int8_t*> buffer = arena.allocateMemory(static_cast<size_t>(dataType.count) * elementSize);
        nautilus::invoke({nautilus::ModRefInfo::Ref}, fixedSizedProxy, fieldIndex, rawBufferIndex, indexer, buffer);
        const FixedSizedData fixedArray{buffer, dataType.count, dataType.elementType};
        return VarVal{fixedArray, dataType.nullable, nautilus::val<bool>{false}};
    }

    static VarVal wrapStruct(
        const DataType& dataType,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer,
        ArenaRef& arena)
    {
        /// Total inline size folds to a host-side constant — fields are schema, not data.
        size_t totalBytes = 0;
        for (const auto& [name, fieldType] : dataType.fields)
        {
            totalBytes += inlineFieldSizeInBytes(fieldType);
        }
        const nautilus::val<int8_t*> buffer = arena.allocateMemory(totalBytes);
        nautilus::invoke({nautilus::ModRefInfo::Ref}, structProxy, fieldIndex, rawBufferIndex, indexer, buffer);
        const StructData structValue{buffer, dataType.fields};
        return VarVal{structValue, dataType.nullable, nautilus::val<bool>{false}};
    }

    /// Top-level entry called from the formatter's `readSpanningRecord`.
    static void writeValueToRecord(
        const DataType& dataType,
        Record& record,
        const std::string& fieldName,
        const nautilus::val<FieldIndex>& fieldIndex,
        const nautilus::val<RawBufferIndex*>& rawBufferIndex,
        const nautilus::val<const InputFormatIndexer*>& indexer,
        ArenaRef& arena)
    {
        record.write(fieldName, parseField(dataType, fieldIndex, rawBufferIndex, indexer, arena));
    }
};

}
