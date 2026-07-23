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

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/TypeTraits.hpp>
#include <ErrorHandling.hpp>

namespace NES::Testing
{

using FieldValue = std::variant<bool, int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, float, double, std::string>;

namespace detail
{
template <typename T>
constexpr DataType::Type expectedType()
{
    if constexpr (std::is_same_v<T, bool>)
    {
        return DataType::Type::BOOLEAN;
    }
    else if constexpr (std::is_same_v<T, int8_t>)
    {
        return DataType::Type::INT8;
    }
    else if constexpr (std::is_same_v<T, int16_t>)
    {
        return DataType::Type::INT16;
    }
    else if constexpr (std::is_same_v<T, int32_t>)
    {
        return DataType::Type::INT32;
    }
    else if constexpr (std::is_same_v<T, int64_t>)
    {
        return DataType::Type::INT64;
    }
    else if constexpr (std::is_same_v<T, uint8_t>)
    {
        return DataType::Type::UINT8;
    }
    else if constexpr (std::is_same_v<T, uint16_t>)
    {
        return DataType::Type::UINT16;
    }
    else if constexpr (std::is_same_v<T, uint32_t>)
    {
        return DataType::Type::UINT32;
    }
    else if constexpr (std::is_same_v<T, uint64_t>)
    {
        return DataType::Type::UINT64;
    }
    else if constexpr (std::is_same_v<T, float>)
    {
        return DataType::Type::FLOAT32;
    }
    else if constexpr (std::is_same_v<T, double>)
    {
        return DataType::Type::FLOAT64;
    }
    else if constexpr (std::is_same_v<T, std::string>)
    {
        return DataType::Type::VARSIZED;
    }
    else
    {
        static_assert(!sizeof(T*), "Unsupported type for FieldView::as<T>()");
    }
}
}

class TestTupleBufferView;
class TestTupleBufferRecordView;
class FieldView;

using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;

/// Entry point. Holds the physical buffer layout (a lowered TupleBufferRef) — no logical schema.
class TestTupleBuffer
{
public:
    /// Constructs from a physical buffer layout, e.g., produced by LowerSchemaProvider::lowerSchema.
    explicit TestTupleBuffer(std::shared_ptr<TupleBufferRef> bufferRef);

    /// Convenience factory: lowers the schema to a row-layout TupleBufferRef for buffers of `bufferSize` bytes.
    static TestTupleBuffer fromSchema(const TestSchema& schema, uint64_t bufferSize);

    /// Wraps an existing TupleBuffer for layout-aware access.
    /// bufferProvider required for VARSIZED (string) field support.
    /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations) convenience default for non-VARSIZED use
    TestTupleBufferView open(TupleBuffer& buffer, AbstractBufferProvider* bufferProvider = nullptr);

private:
    std::shared_ptr<TupleBufferRef> bufferRef;
};

/// View over a TupleBuffer. Supports append and indexed record access.
class TestTupleBufferView
{
public:
    /// Access record at index. Throws if index >= getNumberOfTuples().
    TestTupleBufferRecordView operator[](size_t index);

    /// Append a record with values matching layout fields left-to-right.
    /// Supports implicit numeric casting: append(10, 200) works when the
    /// layout declares INT64 and UINT32 fields.
    template <typename... Args>
    void append(Args&&... values);

    [[nodiscard]] uint64_t getNumberOfTuples() const;

private:
    friend class TestTupleBuffer;
    friend class TestTupleBufferRecordView;
    friend class FieldView;

    struct Impl
    {
        std::vector<Record::RecordFieldIdentifier> fieldNames;
        std::vector<DataType> fieldTypes;
        TupleBuffer&
            buffer; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) intentional reference: Impl always outlived by the TupleBuffer it wraps
        AbstractBufferProvider* bufferProvider;
        std::shared_ptr<TupleBufferRef> bufRef;
    };

    std::shared_ptr<Impl> impl;

    /// Append helper — writes each (possibly null) FieldValue as a full Record via bufRef->writeRecord.
    void appendImpl(std::span<const std::optional<FieldValue>> values);

    /// Converts an argument to a (possibly null) FieldValue, casting to the layout's expected type.
    /// Accepts plain values (T), `std::nullopt` for null, and `std::optional<T>` for either.
    template <typename T>
    static std::optional<FieldValue> toFieldValue(T&& arg, DataType::Type targetType);
};

/// View over a single record.
class TestTupleBufferRecordView
{
public:
    /// Access field by name. Throws if field not in the layout.
    FieldView operator[](const std::string& fieldName);

private:
    friend class TestTupleBufferView;

    std::shared_ptr<TestTupleBufferView::Impl> impl;
    size_t recordIndex;
};

/// Proxy for a single field.
class FieldView
{
public:
    /// Write a non-null value — type-checked against the layout DataType. Works for
    /// both nullable and non-nullable fields.
    FieldView& operator=(const FieldValue& value);

    /// Write null — only valid for fields declared nullable in the layout.
    FieldView& operator=(std::nullopt_t);

    /// Read a typed value. T must match the field's `DataType::Type`.
    /// - `as<T>()` reads a non-nullable field.
    /// - `as<std::optional<T>>()` reads a nullable field, returning `nullopt` when null.
    /// Throws if T does not match the layout type or the layout's nullability.
    template <typename T>
    T as() const;

private:
    friend class TestTupleBufferRecordView;

    [[nodiscard]] std::optional<FieldValue> readFieldValue() const;

    std::weak_ptr<TestTupleBufferView::Impl> implWeak;
    size_t recordIndex;
    std::string fieldName;
    DataType dataType;
};

/// Template implementations

template <typename T>
std::optional<FieldValue> TestTupleBufferView::toFieldValue(T&& arg, DataType::Type targetType)
{
    using Raw = std::remove_cvref_t<T>;

    /// Explicit null sentinel.
    if constexpr (std::is_same_v<Raw, std::nullopt_t>)
    {
        return std::nullopt;
    }
    /// std::optional<U> — recurse on the value, propagate nullopt.
    else if constexpr (IsOptional<Raw>::value)
    {
        if (!arg.has_value())
        {
            return std::nullopt;
        }
        return toFieldValue(*std::forward<T>(arg), targetType);
    }
    /// Pass through if already a FieldValue.
    else if constexpr (std::is_same_v<Raw, FieldValue>)
    {
        return std::optional<FieldValue>{std::forward<T>(arg)};
    }
    /// String-like types (std::string, const char*, string literals).
    else if constexpr (std::is_convertible_v<Raw, std::string_view> && !std::is_arithmetic_v<Raw>)
    {
        if (targetType != DataType::Type::VARSIZED)
        {
            throw DifferentFieldTypeExpected("Cannot assign string value to non-VARSIZED field");
        }
        return FieldValue(std::string(std::forward<T>(arg)));
    }
    /// Bool must be checked before arithmetic since bool is arithmetic.
    else if constexpr (std::is_same_v<Raw, bool>)
    {
        if (targetType != DataType::Type::BOOLEAN)
        {
            throw DifferentFieldTypeExpected("Cannot assign bool to non-BOOLEAN field");
        }
        return FieldValue(arg);
    }
    /// Arithmetic types — cast to the layout's expected type.
    else if constexpr (std::is_arithmetic_v<Raw>)
    {
        switch (targetType)
        {
            case DataType::Type::BOOLEAN:
                return FieldValue(static_cast<bool>(arg));
            case DataType::Type::INT8:
                return FieldValue(static_cast<int8_t>(arg));
            case DataType::Type::INT16:
                return FieldValue(static_cast<int16_t>(arg));
            case DataType::Type::INT32:
                return FieldValue(static_cast<int32_t>(arg));
            case DataType::Type::INT64:
                return FieldValue(static_cast<int64_t>(arg));
            case DataType::Type::UINT8:
                return FieldValue(static_cast<uint8_t>(arg));
            case DataType::Type::UINT16:
                return FieldValue(static_cast<uint16_t>(arg));
            case DataType::Type::UINT32:
                return FieldValue(static_cast<uint32_t>(arg));
            case DataType::Type::UINT64:
                return FieldValue(static_cast<uint64_t>(arg));
            case DataType::Type::FLOAT32:
                return FieldValue(static_cast<float>(arg));
            case DataType::Type::FLOAT64:
                return FieldValue(static_cast<double>(arg));
            default:
                throw DifferentFieldTypeExpected("Cannot cast arithmetic value to target field type");
        }
    }
    else
    {
        static_assert(!sizeof(T*), "Unsupported type for TestTupleBuffer append");
    }
}

template <typename... Args>
void TestTupleBufferView::append(Args&&... values)
{
    static_assert(sizeof...(Args) > 0, "append requires at least one argument");
    if (sizeof...(Args) != impl->fieldTypes.size())
    {
        throw TestException("append requires exactly {} arguments, got {}", impl->fieldTypes.size(), sizeof...(Args));
    }
    auto typeIt = impl->fieldTypes.begin();
    const std::array<std::optional<FieldValue>, sizeof...(Args)> fieldValues{toFieldValue(std::forward<Args>(values), (typeIt++)->type)...};
    appendImpl(fieldValues);
}

template <typename T>
T FieldView::as() const
{
    if constexpr (IsOptional<T>::value)
    {
        using Inner = typename T::value_type;
        if (!dataType.nullable)
        {
            throw TestException("FieldView::as<std::optional<T>>(): field '{}' is non-nullable — use as<T>() instead", fieldName);
        }
        if (dataType.type != detail::expectedType<Inner>())
        {
            throw TestException("FieldView::as<std::optional<T>>(): T does not match the layout type of field '{}'", fieldName);
        }
        auto value = readFieldValue();
        if (!value.has_value())
        {
            return T{};
        }
        return T{std::get<Inner>(*value)};
    }
    else
    {
        if (dataType.nullable)
        {
            throw TestException("FieldView::as<T>(): field '{}' is nullable — use as<std::optional<T>>() instead", fieldName);
        }
        if (dataType.type != detail::expectedType<T>())
        {
            throw TestException("FieldView::as<T>(): T does not match the layout type of field '{}'", fieldName);
        }
        auto value = readFieldValue();
        return std::get<T>(*value);
    }
}

}
