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

#include <TestTupleBuffer.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

namespace NES::Testing
{

namespace
{

/// Checks that the FieldValue variant alternative matches the expected DataType.
bool isTypeCompatible(const DataType::Type type, const FieldValue& value)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return std::holds_alternative<bool>(value);
        case DataType::Type::INT8:
            return std::holds_alternative<int8_t>(value);
        case DataType::Type::INT16:
            return std::holds_alternative<int16_t>(value);
        case DataType::Type::INT32:
            return std::holds_alternative<int32_t>(value);
        case DataType::Type::INT64:
            return std::holds_alternative<int64_t>(value);
        case DataType::Type::UINT8:
            return std::holds_alternative<uint8_t>(value);
        case DataType::Type::UINT16:
            return std::holds_alternative<uint16_t>(value);
        case DataType::Type::UINT32:
            return std::holds_alternative<uint32_t>(value);
        case DataType::Type::UINT64:
            return std::holds_alternative<uint64_t>(value);
        case DataType::Type::FLOAT32:
            return std::holds_alternative<float>(value);
        case DataType::Type::FLOAT64:
            return std::holds_alternative<double>(value);
        case DataType::Type::VARSIZED:
            return std::holds_alternative<std::string>(value);
        default:
            return false;
    }
}

/// Builds a non-null `VarVal` payload from the variant's currently-held alternative.
VarVal nonNullVarVal(const FieldValue& value, const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return VarVal(std::get<bool>(value));
        case DataType::Type::INT8:
            return VarVal(std::get<int8_t>(value));
        case DataType::Type::INT16:
            return VarVal(std::get<int16_t>(value));
        case DataType::Type::INT32:
            return VarVal(std::get<int32_t>(value));
        case DataType::Type::INT64:
            return VarVal(std::get<int64_t>(value));
        case DataType::Type::UINT8:
            return VarVal(std::get<uint8_t>(value));
        case DataType::Type::UINT16:
            return VarVal(std::get<uint16_t>(value));
        case DataType::Type::UINT32:
            return VarVal(std::get<uint32_t>(value));
        case DataType::Type::UINT64:
            return VarVal(std::get<uint64_t>(value));
        case DataType::Type::FLOAT32:
            return VarVal(std::get<float>(value));
        case DataType::Type::FLOAT64:
            return VarVal(std::get<double>(value));
        case DataType::Type::VARSIZED: {
            const auto& str = std::get<std::string>(value);
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast,cppcoreguidelines-pro-type-const-cast) nautilus val<int8_t*> requires non-const int8_t* but std::string exposes const char*
            auto ptr = nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(const_cast<char*>(str.data())));
            auto size = nautilus::val<uint64_t>(str.size());
            return VariableSizedData(ptr, size);
        }
        default:
            throw UnknownDataType("nonNullVarVal: unsupported data type");
    }
}

/// Builds a placeholder `VarVal` carrying the null bit. The payload is irrelevant — the
/// nullable write path stores the null byte first and `loadValue` skips the payload on read.
VarVal nullVarVal(const DataType::Type type)
{
    const auto t = nautilus::val<bool>(true);
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return VarVal(false, /*nullable=*/true, t);
        case DataType::Type::INT8:
            return VarVal(int8_t{0}, /*nullable=*/true, t);
        case DataType::Type::INT16:
            return VarVal(int16_t{0}, /*nullable=*/true, t);
        case DataType::Type::INT32:
            return VarVal(int32_t{0}, /*nullable=*/true, t);
        case DataType::Type::INT64:
            return VarVal(int64_t{0}, /*nullable=*/true, t);
        case DataType::Type::UINT8:
            return VarVal(uint8_t{0}, /*nullable=*/true, t);
        case DataType::Type::UINT16:
            return VarVal(uint16_t{0}, /*nullable=*/true, t);
        case DataType::Type::UINT32:
            return VarVal(uint32_t{0}, /*nullable=*/true, t);
        case DataType::Type::UINT64:
            return VarVal(uint64_t{0}, /*nullable=*/true, t);
        case DataType::Type::FLOAT32:
            return VarVal(float{0}, /*nullable=*/true, t);
        case DataType::Type::FLOAT64:
            return VarVal(double{0}, /*nullable=*/true, t);
        case DataType::Type::VARSIZED: {
            auto ptr = nautilus::val<int8_t*>(nullptr);
            auto size = nautilus::val<uint64_t>(0);
            return VarVal{VariableSizedData(ptr, size), /*nullable=*/true, t};
        }
        default:
            throw UnknownDataType("nullVarVal: unsupported data type");
    }
}

/// Converts a (possibly null) FieldValue to a VarVal honoring the field's nullability.
VarVal fieldValueToVarVal(const std::optional<FieldValue>& value, const DataType& dataType)
{
    if (!value.has_value())
    {
        if (!dataType.nullable)
        {
            throw DifferentFieldTypeExpected("Cannot write null to a non-nullable field");
        }
        return nullVarVal(dataType.type);
    }
    return nonNullVarVal(*value, dataType.type);
}

/// Converts a VarVal back to a FieldValue, returning nullopt if the field is null.
std::optional<FieldValue> varValToFieldValue(const VarVal& value, const DataType& dataType)
{
    if (dataType.nullable && nautilus::details::RawValueResolver<bool>::getRawValue(value.isNull()))
    {
        return std::nullopt;
    }
    switch (dataType.type)
    {
        case DataType::Type::BOOLEAN:
            return FieldValue{nautilus::details::RawValueResolver<bool>::getRawValue(value.getRawValueAs<nautilus::val<bool>>())};
        case DataType::Type::INT8:
            return FieldValue{nautilus::details::RawValueResolver<int8_t>::getRawValue(value.getRawValueAs<nautilus::val<int8_t>>())};
        case DataType::Type::INT16:
            return FieldValue{nautilus::details::RawValueResolver<int16_t>::getRawValue(value.getRawValueAs<nautilus::val<int16_t>>())};
        case DataType::Type::INT32:
            return FieldValue{nautilus::details::RawValueResolver<int32_t>::getRawValue(value.getRawValueAs<nautilus::val<int32_t>>())};
        case DataType::Type::INT64:
            return FieldValue{nautilus::details::RawValueResolver<int64_t>::getRawValue(value.getRawValueAs<nautilus::val<int64_t>>())};
        case DataType::Type::UINT8:
            return FieldValue{nautilus::details::RawValueResolver<uint8_t>::getRawValue(value.getRawValueAs<nautilus::val<uint8_t>>())};
        case DataType::Type::UINT16:
            return FieldValue{nautilus::details::RawValueResolver<uint16_t>::getRawValue(value.getRawValueAs<nautilus::val<uint16_t>>())};
        case DataType::Type::UINT32:
            return FieldValue{nautilus::details::RawValueResolver<uint32_t>::getRawValue(value.getRawValueAs<nautilus::val<uint32_t>>())};
        case DataType::Type::UINT64:
            return FieldValue{nautilus::details::RawValueResolver<uint64_t>::getRawValue(value.getRawValueAs<nautilus::val<uint64_t>>())};
        case DataType::Type::FLOAT32:
            return FieldValue{nautilus::details::RawValueResolver<float>::getRawValue(value.getRawValueAs<nautilus::val<float>>())};
        case DataType::Type::FLOAT64:
            return FieldValue{nautilus::details::RawValueResolver<double>::getRawValue(value.getRawValueAs<nautilus::val<double>>())};
        case DataType::Type::VARSIZED: {
            auto varSized = value.getRawValueAs<VariableSizedData>();
            auto* contentPtr = nautilus::details::RawValueResolver<int8_t*>::getRawValue(varSized.getContent());
            auto size = nautilus::details::RawValueResolver<uint64_t>::getRawValue(varSized.getSize());
            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) int8_t* to const char* for string construction
            return FieldValue{std::string(reinterpret_cast<const char*>(contentPtr), size)};
        }
        default:
            throw UnknownDataType("varValToFieldValue: unsupported data type");
    }
}

} /// anonymous namespace

/// ---- TestTupleBuffer ----

TestTupleBuffer::TestTupleBuffer(const Schema& schema) : schema(schema)
{
}

TestTupleBufferView TestTupleBuffer::open(TupleBuffer& buffer, AbstractBufferProvider* bufferProvider)
{
    auto bufRef = LowerSchemaProvider::lowerSchema(buffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);

    TestTupleBufferView view;
    view.impl = std::make_shared<TestTupleBufferView::Impl>(
        TestTupleBufferView::Impl{.schema = schema, .buffer = buffer, .bufferProvider = bufferProvider, .bufRef = std::move(bufRef)});
    return view;
}

/// ---- TestTupleBufferView ----

TestTupleBufferRecordView TestTupleBufferView::operator[](const size_t index)
{
    if (index >= impl->buffer.getNumberOfTuples())
    {
        throw TestException("TestTupleBufferView: index {} out of bounds, buffer has {} tuples", index, impl->buffer.getNumberOfTuples());
    }
    TestTupleBufferRecordView recordView;
    recordView.impl = impl;
    recordView.recordIndex = index;
    return recordView;
}

uint64_t TestTupleBufferView::getNumberOfTuples() const
{
    return impl->buffer.getNumberOfTuples();
}

void TestTupleBufferView::appendImpl(std::span<const std::optional<FieldValue>> values)
{
    if (values.size() != impl->schema.getNumberOfFields())
    {
        throw TestException("TestTupleBufferView: expected {} fields, got {}", impl->schema.getNumberOfFields(), values.size());
    }

    auto tupleIndex = nautilus::val<uint64_t>(impl->buffer.getNumberOfTuples());
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(impl->buffer));
    const RecordBuffer recordBuffer(bufPtr);
    auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(impl->bufferProvider);

    Record record;
    for (const auto [value, field] : std::views::zip(values, impl->schema.getFields()))
    {
        record.write(field.name, fieldValueToVarVal(value, field.dataType));
    }

    impl->bufRef->writeRecord(tupleIndex, recordBuffer, record, bufProviderVal);
    impl->buffer.setNumberOfTuples(impl->buffer.getNumberOfTuples() + 1);
}

/// ---- TestTupleBufferRecordView ----

FieldView TestTupleBufferRecordView::operator[](const std::string& fieldName)
{
    const auto& fields = impl->schema.getFields();
    const auto it = std::ranges::find(fields, fieldName, &Schema::Field::name);
    if (it == fields.end())
    {
        throw FieldNotFound("TestTupleBufferRecordView: field '{}' not found in schema", fieldName);
    }

    FieldView fieldView;
    fieldView.implWeak = impl;
    fieldView.recordIndex = recordIndex;
    fieldView.fieldName = fieldName;
    fieldView.dataType = it->dataType;
    return fieldView;
}

/// ---- FieldView ----

FieldView& FieldView::operator=(const FieldValue& value)
{
    auto locked = implWeak.lock();
    if (locked == nullptr)
    {
        throw TestException("FieldView: dangling reference — parent view has been destroyed");
    }
    if (!isTypeCompatible(dataType.type, value))
    {
        throw DifferentFieldTypeExpected("FieldView: type mismatch when assigning to field '{}'", fieldName);
    }

    auto recordIdx = nautilus::val<uint64_t>(recordIndex);
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(locked->buffer));
    const RecordBuffer recordBuffer(bufPtr);
    auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(locked->bufferProvider);

    Record record;
    record.write(fieldName, fieldValueToVarVal(std::optional{value}, dataType));
    locked->bufRef->writeRecord(recordIdx, recordBuffer, record, bufProviderVal);
    return *this;
}

FieldView& FieldView::operator=(std::nullopt_t)
{
    auto locked = implWeak.lock();
    if (locked == nullptr)
    {
        throw TestException("FieldView: dangling reference — parent view has been destroyed");
    }
    if (!dataType.nullable)
    {
        throw DifferentFieldTypeExpected("FieldView: cannot assign null to non-nullable field '{}'", fieldName);
    }

    auto recordIdx = nautilus::val<uint64_t>(recordIndex);
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(locked->buffer));
    const RecordBuffer recordBuffer(bufPtr);
    auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(locked->bufferProvider);

    Record record;
    record.write(fieldName, fieldValueToVarVal(std::nullopt, dataType));
    locked->bufRef->writeRecord(recordIdx, recordBuffer, record, bufProviderVal);
    return *this;
}

std::optional<FieldValue> FieldView::readFieldValue() const
{
    auto locked = implWeak.lock();
    if (locked == nullptr)
    {
        throw TestException("FieldView: dangling reference — parent view has been destroyed");
    }

    auto recordIdx = nautilus::val<uint64_t>(recordIndex);
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(locked->buffer));
    const RecordBuffer recordBuffer(bufPtr);

    auto record = locked->bufRef->readRecord({fieldName}, recordBuffer, recordIdx);
    const auto& varVal = record.read(fieldName);
    return varValToFieldValue(varVal, dataType);
}

}
