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
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Testing
{

using FieldValue = std::variant<bool, int8_t, int16_t, int32_t, int64_t, uint8_t, uint16_t, uint32_t, uint64_t, float, double, std::string>;

class TestTupleBufferView;
class TestTupleBufferRecordView;
class FieldView;

/// Entry point. Holds schema config.
class TestTupleBuffer
{
public:
    explicit TestTupleBuffer(Schema schema);

    /// Wraps an existing TupleBuffer for schema-aware access.
    /// bufferProvider required for VARSIZED (string) field support.
    TestTupleBufferView open(TupleBuffer& buffer, AbstractBufferProvider* bufferProvider = nullptr);

private:
    Schema schema;
};

/// View over a TupleBuffer. Supports append and indexed record access.
class TestTupleBufferView
{
public:
    /// Access record at index. Throws if index >= getNumberOfTuples().
    TestTupleBufferRecordView operator[](size_t index);

    /// Append a record with values matching schema fields left-to-right.
    /// Supports implicit numeric casting: append(10, 200) works when the
    /// schema declares INT64 and UINT32 fields.
    template <typename... Args>
    void append(Args&&... values);

    [[nodiscard]] uint64_t getNumberOfTuples() const;

private:
    friend class TestTupleBuffer;
    friend class TestTupleBufferRecordView;
    friend class FieldView;

    struct Impl
    {
        Schema schema;
        TupleBuffer& buffer;
        AbstractBufferProvider* bufferProvider;
        std::shared_ptr<TupleBufferRef> bufRef;
    };

    std::shared_ptr<Impl> impl;

    /// Append helper — writes each FieldValue as a full Record via bufRef->writeRecord.
    void appendImpl(const FieldValue* values, size_t count);

    /// Converts an argument to a FieldValue, casting to the schema's expected type.
    template <typename T>
    static FieldValue toFieldValue(T&& arg, DataType::Type targetType);
};

/// View over a single record.
class TestTupleBufferRecordView
{
public:
    /// Access field by name. Throws if field not in schema.
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
    /// Write — type-checked against schema DataType.
    FieldView& operator=(FieldValue value);

    /// Read — extracts raw C++ value. Throws if T does not match the field's DataType.
    template <typename T>
    T as() const;

    /// Read as FieldValue variant.
    [[nodiscard]] FieldValue get() const;

private:
    friend class TestTupleBufferRecordView;

    std::weak_ptr<TestTupleBufferView::Impl> implWeak;
    size_t recordIndex;
    std::string fieldName;
    DataType dataType;
};

/// Template implementations

template <typename T>
FieldValue TestTupleBufferView::toFieldValue(T&& arg, DataType::Type targetType)
{
    using Raw = std::remove_cvref_t<T>;

    /// Pass through if already a FieldValue.
    if constexpr (std::is_same_v<Raw, FieldValue>)
    {
        return std::forward<T>(arg);
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
    /// Arithmetic types — cast to the schema's expected type.
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
    if (sizeof...(Args) != impl->schema.getNumberOfFields())
    {
        throw TestException("append requires exactly {} arguments, got {}", impl->schema.getNumberOfFields(), sizeof...(Args));
    }
    const auto& fields = impl->schema.getFields();
    size_t i = 0;
    const FieldValue fieldValues[] = {toFieldValue(std::forward<Args>(values), fields[i++].dataType.type)...};
    appendImpl(fieldValues, sizeof...(Args));
}

template <typename T>
T FieldView::as() const
{
    auto value = get();
    if (!std::holds_alternative<T>(value))
    {
        throw TestException("FieldView::as<T>(): requested type does not match field type for '{}'", fieldName);
    }
    return std::get<T>(value);
}

} /// namespace NES::Testing
