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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/AbstractJavaUDFOperator.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Runtime::Execution::Operators {

AbstractJavaUDFOperator::AbstractJavaUDFOperator(uint64_t operatorHandlerIndex,
                                                 NES::SchemaPtr operatorInputSchema,
                                                 NES::SchemaPtr operatorOutputSchema)
    : operatorHandlerIndex(operatorHandlerIndex), operatorInputSchema(std::move(operatorInputSchema)),
      operatorOutputSchema(std::move(operatorOutputSchema)){};

void* getInstance(void* state) {
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    return handler->getUdfInstance();
}

void setupHandler(void* state) {
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    handler->setup();
}

void AbstractJavaUDFOperator::setup(ExecutionContext& ctx) const {
    Operator::setup(ctx);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("setupHandler", setupHandler, handler);
}

void AbstractJavaUDFOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    Operator::open(ctx, recordBuffer);
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto udfInstance = FunctionCall<>("getInstance", getInstance, handler);
    ctx.setLocalOperatorState(this, std::make_unique<LocalUDFState>(handler, udfInstance));
}

void AbstractJavaUDFOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    FunctionCall<>("detachJVM", jni::detachEnv);
    Operator::close(executionCtx, recordBuffer);
}

Record AbstractJavaUDFOperator::extractRecordFromPojo(const Value<MemRef>& handler, const Value<MemRef>& outputPojoPtr) const {
    // Create new record for result
    auto resultRecord = Record();

    // Reading result values from jvm into result record
    // Same differentiation as for input class above
    if (operatorOutputSchema->fields.size() == 1) {
        // 1. Simple, the output schema contains only one field
        const auto field = operatorOutputSchema->fields[0];
        const auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            Value<> val = FunctionCall<>("getBooleanObjectValue", getBooleanObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            Value<> val = FunctionCall<>("getFloatObjectValue", getFloatObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            Value<> val = FunctionCall<>("getDoubleObjectValue", getDoubleObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            Value<> val = FunctionCall<>("getIntegerObjectValue", getIntegerObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            Value<> val = FunctionCall<>("getLongObjectValue", getLongObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            Value<> val = FunctionCall<>("getShortObjectValue", getShortObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            Value<> val = FunctionCall<>("getByteObjectValue", getByteObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            Value<> val = FunctionCall<>("getStringObjectValue", getStringObjectValue, outputPojoPtr);
            resultRecord.write(fieldName, val);
        } else {
            NES_THROW_RUNTIME_ERROR("Could not extract value from Java UDF output object (data type not supported): type="
                                    << std::string(field->getDataType()->toString()));
        }
    } else {
        auto outputClassPtr = FunctionCall<>("getObjectClass", getObjectClass, outputPojoPtr);
        // 2. Complex, a plain old java object with multiple primitive types as map output
        for (int i = 0; i < (int) operatorOutputSchema->fields.size(); i++) {
            const auto field = operatorOutputSchema->fields[i];
            const auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                Value<> val =
                    FunctionCall<>("getBooleanField", getBooleanField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                Value<> val =
                    FunctionCall<>("getFloatField", getFloatField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                Value<> val =
                    FunctionCall<>("getDoubleField", getDoubleField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                Value<> val =
                    FunctionCall<>("getIntegerField", getIntegerField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                Value<> val =
                    FunctionCall<>("getLongField", getLongField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                Value<> val =
                    FunctionCall<>("getShortField", getShortField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                Value<> val =
                    FunctionCall<>("getByteField", getByteField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                Value<> val =
                    FunctionCall<>("getStringField", getStringField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                resultRecord.write(fieldName, val);
            } else {
                NES_THROW_RUNTIME_ERROR("Could not extract value from Java UDF output POJO (data type not supported): field="
                                        << fieldName << ", type=" << std::string(field->getDataType()->toString()));
            }
        }
    }
    return resultRecord;
}

Nautilus::Value<MemRef> AbstractJavaUDFOperator::createInputPojo(Record& record, Value<MemRef>& handler) const {
    // Loading record values into java input class
    // We derive the types of the values from the schema. The type can be complex of simple.
    // 1. Simple: tuples with one field represented through an object type (String, Integer, ..)
    // 2. Complex: plain old java object containing the multiple primitive types
    if (operatorInputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        auto field = operatorInputSchema->fields[0];
        // Record should contain only one field
        assert(record.getAllFields().size() == 1);
        auto fieldName = record.getAllFields()[0];
        const auto type = field->getDataType();
        if (type->isEquals(DataTypeFactory::createBoolean())) {
            return FunctionCall<>("createBooleanObject", createBooleanObject, record.read(fieldName).as<Boolean>());
        } else if (type->isEquals(DataTypeFactory::createFloat())) {
            return FunctionCall<>("createFloatObject", createFloatObject, record.read(fieldName).as<Float>());
        } else if (type->isEquals(DataTypeFactory::createDouble())) {
            return FunctionCall<>("createDoubleObject", createDoubleObject, record.read(fieldName).as<Double>());
        } else if (type->isEquals(DataTypeFactory::createInt32())) {
            return FunctionCall<>("createIntegerObject", createIntegerObject, record.read(fieldName).as<Int32>());
        } else if (type->isEquals(DataTypeFactory::createInt64()) || type->isEquals(DataTypeFactory::createUInt64())) {
            return FunctionCall<>("createLongObject", createLongObject, record.read(fieldName).as<Int64>());
        } else if (type->isEquals(DataTypeFactory::createInt16())) {
            return FunctionCall<>("createShortObject", createShortObject, record.read(fieldName).as<Int16>());
        } else if (type->isEquals(DataTypeFactory::createInt8())) {
            return FunctionCall<>("createByteObject", createByteObject, record.read(fieldName).as<Int8>());
        } else if (type->isEquals(DataTypeFactory::createText())) {
            return FunctionCall<>("createStringObject", createStringObject, record.read(fieldName).as<Text>()->getReference());
        } else {
            NES_THROW_RUNTIME_ERROR(
                "Could not create Java UDF input object (data type not supported): type=" << std::string(type->toString()));
        }
    } else {
        auto inputClassPtr = FunctionCall("findInputClass", findInputClass, handler);
        auto inputPojoPtr = FunctionCall("allocateObject", allocateObject, inputClassPtr);
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) operatorInputSchema->fields.size(); i++) {
            auto field = operatorInputSchema->fields[i];
            auto fieldName = field->getName();
            // TODO reduce code and collapsed proxy functions.
            const auto type = field->getDataType();
            if (type->isEquals(DataTypeFactory::createBoolean())) {
                FunctionCall<>("setBooleanField",
                               setBooleanField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Boolean>());
            } else if (type->isEquals(DataTypeFactory::createFloat())) {
                FunctionCall<>("setFloatField",
                               setFloatField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Float>());
            } else if (type->isEquals(DataTypeFactory::createDouble())) {
                FunctionCall<>("setDoubleField",
                               setDoubleField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Double>());
            } else if (type->isEquals(DataTypeFactory::createInt32())) {
                FunctionCall<>("setIntegerField",
                               setIntegerField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int32>());
            } else if (type->isEquals(DataTypeFactory::createInt64()) || type->isEquals(DataTypeFactory::createUInt64())) {
                FunctionCall<>("setLongField",
                               setLongField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int64>());
            } else if (type->isEquals(DataTypeFactory::createInt16())) {
                FunctionCall<>("setShortField",
                               setShortField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int16>());
            } else if (type->isEquals(DataTypeFactory::createInt8())) {
                FunctionCall<>("setByteField",
                               setByteField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int8>());
            } else if (type->isEquals(DataTypeFactory::createText())) {
                FunctionCall<>("setStringField",
                               setStringField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Text>()->getReference());
            } else {
                NES_THROW_RUNTIME_ERROR("Could not create Java UDF input POJO (data type not supported): field="
                                        << fieldName << ", type=" << std::string(type->toString()));
            }
        }
        return inputPojoPtr;
    }
}

}// namespace NES::Runtime::Execution::Operators