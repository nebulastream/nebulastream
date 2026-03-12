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
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>

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

/// Converts a FieldValue to a VarVal for use with the Record/TupleBufferRef interface.
VarVal fieldValueToVarVal(const FieldValue& value, const DataType::Type type)
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
            auto ptr = nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(const_cast<char*>(str.data())));
            auto size = nautilus::val<uint64_t>(str.size());
            return VariableSizedData(ptr, size);
        }
        default:
            throw UnknownDataType("fieldValueToVarVal: unsupported data type");
    }
}

/// Converts a VarVal back to a FieldValue by extracting the raw C++ value.
FieldValue varValToFieldValue(const VarVal& value, const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
            return nautilus::details::RawValueResolver<bool>::getRawValue(value.cast<nautilus::val<bool>>());
        case DataType::Type::INT8:
            return nautilus::details::RawValueResolver<int8_t>::getRawValue(value.cast<nautilus::val<int8_t>>());
        case DataType::Type::INT16:
            return nautilus::details::RawValueResolver<int16_t>::getRawValue(value.cast<nautilus::val<int16_t>>());
        case DataType::Type::INT32:
            return nautilus::details::RawValueResolver<int32_t>::getRawValue(value.cast<nautilus::val<int32_t>>());
        case DataType::Type::INT64:
            return nautilus::details::RawValueResolver<int64_t>::getRawValue(value.cast<nautilus::val<int64_t>>());
        case DataType::Type::UINT8:
            return nautilus::details::RawValueResolver<uint8_t>::getRawValue(value.cast<nautilus::val<uint8_t>>());
        case DataType::Type::UINT16:
            return nautilus::details::RawValueResolver<uint16_t>::getRawValue(value.cast<nautilus::val<uint16_t>>());
        case DataType::Type::UINT32:
            return nautilus::details::RawValueResolver<uint32_t>::getRawValue(value.cast<nautilus::val<uint32_t>>());
        case DataType::Type::UINT64:
            return nautilus::details::RawValueResolver<uint64_t>::getRawValue(value.cast<nautilus::val<uint64_t>>());
        case DataType::Type::FLOAT32:
            return nautilus::details::RawValueResolver<float>::getRawValue(value.cast<nautilus::val<float>>());
        case DataType::Type::FLOAT64:
            return nautilus::details::RawValueResolver<double>::getRawValue(value.cast<nautilus::val<double>>());
        case DataType::Type::VARSIZED: {
            auto varSized = value.cast<VariableSizedData>();
            auto contentPtr = nautilus::details::RawValueResolver<int8_t*>::getRawValue(varSized.getContent());
            auto size = nautilus::details::RawValueResolver<uint64_t>::getRawValue(varSized.getSize());
            return std::string(reinterpret_cast<const char*>(contentPtr), size);
        }
        default:
            throw UnknownDataType("varValToFieldValue: unsupported data type");
    }
}

} /// anonymous namespace

/// ---- TestTupleBuffer ----

TestTupleBuffer::TestTupleBuffer(Schema schema) : schema(std::move(schema))
{
}

TestTupleBufferView TestTupleBuffer::open(TupleBuffer& buffer, AbstractBufferProvider* bufferProvider)
{
    auto bufRef = LowerSchemaProvider::lowerSchema(buffer.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);

    TestTupleBufferView view;
    view.impl = std::make_shared<TestTupleBufferView::Impl>(TestTupleBufferView::Impl{schema, buffer, bufferProvider, std::move(bufRef)});
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

void TestTupleBufferView::appendImpl(const FieldValue* values, const size_t count)
{
    if (count != impl->schema.getNumberOfFields())
    {
        throw TestException("TestTupleBufferView: expected {} fields, got {}", impl->schema.getNumberOfFields(), count);
    }

    auto tupleIndex = nautilus::val<uint64_t>(impl->buffer.getNumberOfTuples());
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(impl->buffer));
    RecordBuffer recordBuffer(bufPtr);
    auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(impl->bufferProvider);

    Record record;
    const auto& fields = impl->schema.getFields();
    for (size_t i = 0; i < count; ++i)
    {
        record.write(fields[i].name, fieldValueToVarVal(values[i], fields[i].dataType.type));
    }

    impl->bufRef->writeRecord(tupleIndex, recordBuffer, record, bufProviderVal);
    impl->buffer.setNumberOfTuples(impl->buffer.getNumberOfTuples() + 1);
}

/// ---- TestTupleBufferRecordView ----

FieldView TestTupleBufferRecordView::operator[](const std::string& fieldName)
{
    const auto& fields = impl->schema.getFields();
    auto it = std::find_if(fields.begin(), fields.end(), [&fieldName](const auto& f) { return f.name == fieldName; });
    if (it == fields.end())
    {
        throw FieldNotFound("TestTupleBufferRecordView: field '{}' not found in schema", fieldName);
    }

    FieldView fv;
    fv.implWeak = impl;
    fv.recordIndex = recordIndex;
    fv.fieldName = fieldName;
    fv.dataType = it->dataType;
    return fv;
}

/// ---- FieldView ----

FieldView& FieldView::operator=(FieldValue value)
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
    RecordBuffer recordBuffer(bufPtr);
    auto bufProviderVal = nautilus::val<AbstractBufferProvider*>(locked->bufferProvider);

    Record record;
    record.write(fieldName, fieldValueToVarVal(value, dataType.type));
    locked->bufRef->writeRecord(recordIdx, recordBuffer, record, bufProviderVal);
    return *this;
}

FieldValue FieldView::get() const
{
    auto locked = implWeak.lock();
    if (locked == nullptr)
    {
        throw TestException("FieldView: dangling reference — parent view has been destroyed");
    }

    auto recordIdx = nautilus::val<uint64_t>(recordIndex);
    auto bufPtr = nautilus::val<TupleBuffer*>(std::addressof(locked->buffer));
    RecordBuffer recordBuffer(bufPtr);

    auto record = locked->bufRef->readRecord({fieldName}, recordBuffer, recordIdx);
    const auto& varVal = record.read(fieldName);
    return varValToFieldValue(varVal, dataType.type);
}

} /// namespace NES::Testing
