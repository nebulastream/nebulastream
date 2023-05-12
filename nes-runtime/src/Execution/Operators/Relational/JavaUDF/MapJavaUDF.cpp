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
#ifdef ENABLE_JNI

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>
#if not(defined(__APPLE__))
#include <experimental/source_location>
#endif

namespace NES::Runtime::Execution::Operators {

/**
 * Execute the java udf
 * @param state operator handler state
 * @param pojoObjectPtr pojo object
 * @return result of the udf
 */
void* executeMapUdf(void* state, void* pojoObjectPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(pojoObjectPtr != nullptr, "pojoObjectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    // Find class implementing the map udf
    jclass c1 = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);

    // Build function signature of map function
    std::string sig = "(L" + handler->getInputClassName() + ";)L" + handler->getOutputClassName() + ";";

    // Find udf function
    jmethodID mid = handler->getEnvironment()->GetMethodID(c1, handler->getMethodName().c_str(), sig.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);

    jobject udf_result, instance;
    // The map udf class will be either loaded from a serialized instance or allocated using class information
    if (!handler->getSerializedInstance().empty()) {
        // Load instance if defined
        instance = deserializeInstance(state);
    } else {
        // Create instance object using class information
        jclass clazz = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);

        // Here we assume the default constructor is available
        auto constr = handler->getEnvironment()->GetMethodID(clazz, "<init>", "()V");
        instance = handler->getEnvironment()->NewObject(clazz, constr);
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    }

    // Call udf function
    udf_result = handler->getEnvironment()->CallObjectMethod(instance, mid, pojoObjectPtr);
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return udf_result;
}

/**
 * Operator execution function
 * @param ctx operator context
 * @param record input record
 */
void MapJavaUDF::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // We need to ensure that the current thread is attached to the JVM
    // When we can guarantee that the operator is always executed in the same thread we can remove attaching and detaching
    FunctionCall("startOrAttachVM", startOrAttachVM, handler);

    // Allocate java input class
    auto inputClassPtr = FunctionCall("findInputClass", findInputClass, handler);
    auto inputPojoPtr = FunctionCall("allocateObject", allocateObject, handler, inputClassPtr);

    // Loading record values into java input class
    // We derive the types of the values from the schema. The type can be complex of simple.
    // 1. Simple: tuples with one field represented through an object type (String, Integer, ..)
    // 2. Complex: plain old java object containing the multiple primitive types
    if (operatorInputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        const auto field = operatorInputSchema->fields[0];
        // Record should contain only one field
        assert(record.getAllFields().size() == 1);
        const auto fieldName = record.getAllFields()[0];

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            inputPojoPtr =
                FunctionCall<>("createBooleanObject", createBooleanObject, handler, record.read(fieldName).as<Boolean>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            inputPojoPtr = FunctionCall<>("createFloatObject", createFloatObject, handler, record.read(fieldName).as<Float>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            inputPojoPtr = FunctionCall<>("createDoubleObject", createDoubleObject, handler, record.read(fieldName).as<Double>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            inputPojoPtr =
                FunctionCall<>("createIntegerObject", createIntegerObject, handler, record.read(fieldName).as<Int32>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            inputPojoPtr = FunctionCall<>("createLongObject", createLongObject, handler, record.read(fieldName).as<Int64>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            inputPojoPtr = FunctionCall<>("createShortObject", createShortObject, handler, record.read(fieldName).as<Int16>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            inputPojoPtr = FunctionCall<>("createByteObject", createByteObject, handler, record.read(fieldName).as<Int8>());
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            inputPojoPtr = FunctionCall<>("createStringObject",
                                          createStringObject,
                                          handler,
                                          record.read(fieldName).as<Text>()->getReference());
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) operatorInputSchema->fields.size(); i++) {
            const auto field = operatorInputSchema->fields[i];
            const auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                FunctionCall<>("setBooleanField",
                               setBooleanField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Boolean>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                FunctionCall<>("setFloatField",
                               setFloatField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                FunctionCall<>("setDoubleField",
                               setDoubleField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Double>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                FunctionCall<>("setIntegerField",
                               setIntegerField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int32>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                FunctionCall<>("setLongField",
                               setLongField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int64>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                FunctionCall<>("setShortField",
                               setShortField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int16>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                FunctionCall<>("setByteField",
                               setByteField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int8>());
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                FunctionCall<>("setStringField",
                               setStringField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Text>()->getReference());
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
    }

    // Get output class and call udf
    auto outputClassPtr = FunctionCall<>("findOutputClass", findOutputClass, handler);
    auto outputPojoPtr = FunctionCall<>("executeMapUdf", executeMapUdf, handler, inputPojoPtr);

    FunctionCall<>("freeObject", freeObject, handler, inputPojoPtr);

    // Create new record for result
    record = Record();

    // Reading result values from jvm into result record
    // Same differentiation as for input class above
    if (operatorOutputSchema->fields.size() == 1) {
        // 1. Simple, the output schema contains only one field
        const auto field = operatorOutputSchema->fields[0];
        const auto fieldName = field->getName();

        if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
            Value<> val = FunctionCall<>("getBooleanObjectValue", getBooleanObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
            Value<> val = FunctionCall<>("getFloatObjectValue", getFloatObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
            Value<> val = FunctionCall<>("getDoubleObjectValue", getDoubleObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
            Value<> val = FunctionCall<>("getIntegerObjectValue", getIntegerObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
            Value<> val = FunctionCall<>("getLongObjectValue", getLongObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
            Value<> val = FunctionCall<>("getShortObjectValue", getShortObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
            Value<> val = FunctionCall<>("getByteObjectValue", getByteObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
            Value<> val = FunctionCall<>("getStringObjectValue", getStringObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else {
            NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map output
        for (int i = 0; i < (int) operatorOutputSchema->fields.size(); i++) {
            const auto field = operatorOutputSchema->fields[i];
            const auto fieldName = field->getName();

            if (field->getDataType()->isEquals(DataTypeFactory::createBoolean())) {
                Value<> val =
                    FunctionCall<>("getBooleanField", getBooleanField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createFloat())) {
                Value<> val =
                    FunctionCall<>("getFloatField", getFloatField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createDouble())) {
                Value<> val =
                    FunctionCall<>("getDoubleField", getDoubleField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt32())) {
                Value<> val =
                    FunctionCall<>("getIntegerField", getIntegerField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt64())) {
                Value<> val =
                    FunctionCall<>("getLongField", getLongField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt16())) {
                Value<> val =
                    FunctionCall<>("getShortField", getShortField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createInt8())) {
                Value<> val =
                    FunctionCall<>("getByteField", getByteField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isEquals(DataTypeFactory::createText())) {
                Value<> val =
                    FunctionCall<>("getStringField", getStringField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else {
                NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(field->getDataType()->toString()));
            }
        }
    }

    FunctionCall<>("freeObject", freeObject, handler, outputPojoPtr);
    FunctionCall<>("detachJVM", detachVM);

    // Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

/**
 * Terminate operator
 * @param ctx execution context
 */
void MapJavaUDF::terminate(ExecutionContext&) const {
    // TODO fix usage of jvm
    //FunctionCall<>("destroyVM", destroyVM);
}

}// namespace NES::Runtime::Execution::Operators
#endif// ENABLE_JIN