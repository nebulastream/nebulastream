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

#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/MapJavaUdfOperatorHandler.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JVMContext.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <jni.h>
#include <utility>
#include <experimental/source_location>
#include "boost/filesystem.hpp"

namespace NES::Runtime::Execution::Operators {

inline void jniErrorCheck(void* state, const std::source_location& location = std::source_location::current()) {
    auto handler = (MapJavaUdfOperatorHandler*) state;
    if (handler->getEnvironment()->ExceptionOccurred()) {
        // print the stack trace
        handler->getEnvironment()->ExceptionDescribe();
        NES_FATAL_ERROR("An error occurred during a map java UDF execution in function " << location.function_name());
        exit(EXIT_FAILURE);
    }
}

inline bool dirExists(const std::string& name) { return boost::filesystem::exists(name.c_str()); }

extern "C" void loadClassesFromByteList(void* state, const std::unordered_map<std::string, std::vector<char>>& byteCodeList) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    for (auto entry : byteCodeList) {
        auto bufLen = entry.second.size();
        const auto byteCode = reinterpret_cast<jbyte*>(&entry.second[0]);
        handler->getEnvironment()->DefineClass(entry.first.data(), nullptr, byteCode, (jsize) bufLen);
    }
}

extern "C" jobject deserializeInstance(void* state) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    void *object = (void *)handler->getSerializedInstance().data();
    auto clazz = handler->getEnvironment()->FindClass("MapJavaUdfUtils");
    jniErrorCheck(handler);
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "deserialize", "(Ljava/nio/ByteBuffer;)Ljava/lang/Object;");
    jniErrorCheck(handler);
    auto obj = handler->getEnvironment()->CallStaticObjectMethod(clazz, mid, object);
    jniErrorCheck(handler);
    return obj;
}

extern "C" void startVMWithJarFile(void *state) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    // Sanity check javaPath
    auto javaPath = handler->getJavaPath().value();
    if (!dirExists(javaPath)) {
        NES_FATAL_ERROR("jarPath:" << javaPath << " not valid!");
        exit(EXIT_FAILURE);
    }

    JavaVMInitArgs vmArgs;
    auto* options = new JavaVMOption[3];
    std::string classPathOpt = std::string("-Djava.class.path=") + javaPath;
    options[0].optionString = (char*) classPathOpt.c_str();
    options[1].optionString = (char*) "-verbose:jni";
    options[2].optionString = (char*) "-verbose:class";
    vmArgs.nOptions = 3;
    vmArgs.options = options;
    vmArgs.version = JNI_VERSION_1_2;
    vmArgs.ignoreUnrecognized = true; // invalid options make the JVM init fail

    JVMContext &context = JVMContext::getJVMContext();
    auto env = handler->getEnvironment();
    context.createOrAttachToJVM(&env, vmArgs);
    handler->setEnvironment(env);
}

extern "C" void startVMWithByteList(void *state){
    auto handler = (MapJavaUdfOperatorHandler*) state;

    JavaVMInitArgs vmArgs;
    vmArgs.version = JNI_VERSION_1_2;
    vmArgs.ignoreUnrecognized = true; // invalid options make the JVM init fail

    JVMContext &context = JVMContext::getJVMContext();
    auto env = handler->getEnvironment();
    context.createOrAttachToJVM(&env, vmArgs);
    handler->setEnvironment(env);
    loadClassesFromByteList(handler, handler->getByteCodeList());
}

extern "C" void startVM(void *state) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    if (handler->getJavaPath().has_value()) {
        // Get java classes using jar files
        startVMWithJarFile(state);
    } else {
        // Get java classes using byte list
        startVMWithByteList(state);
    }
}

extern "C" void detachVM(){
    JVMContext &context = JVMContext::getJVMContext();
    context.detachFromJVM();
}

extern "C" void destroyVM(){
    JVMContext &context = JVMContext::getJVMContext();
    context.destroyJVM();
}

extern "C" void* findInputProxyClass(void* state) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    jclass clazz = handler->getEnvironment()->FindClass(handler->getInputClassName().c_str());
    jniErrorCheck(handler);
    return clazz;
}

extern "C" void* findOutputProxyClass(void* state) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    jclass clazz = handler->getEnvironment()->FindClass(handler->getOutputClassName().c_str());
    jniErrorCheck(handler);
    return clazz;
}

extern "C" void* allocateObject(void* state, void* classPtr) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    auto clazz = (jclass) classPtr;
    jobject obj = handler->getEnvironment()->AllocObject(clazz);
    jniErrorCheck(handler);
    return obj;
}

template <typename T>
void *createObjectType(void *state, T value, std::string className, std::string constructorSignature){
    auto handler = (MapJavaUdfOperatorHandler*) state;

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler);
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "<init>", constructorSignature.c_str());
    jniErrorCheck(handler);
    auto object = handler->getEnvironment()->NewObject(clazz, mid, value);
    jniErrorCheck(handler);
    return object;
}

extern "C" void *createFloatObject(void* state, float value) {
    return createObjectType(state, value, "java/lang/Float", "(F)V");
}

extern "C" void *createBooleanObject(void* state, bool value) {
    return createObjectType(state, value, "java/lang/Boolean", "(Z)V");
}

extern "C" void *createIntegerObject(void* state, int32_t value) {
    return createObjectType(state, value, "java/lang/Integer", "(I)V");
}

extern "C" void *createLongObject(void* state, int64_t value) {
    return createObjectType(state, value, "java/lang/Long", "(J)V");
}

extern "C" void *createShortObject(void* state, int16_t value) {
    return createObjectType(state, value, "java/lang/Short", "(S)V");
}

extern "C" void *createDoubleObject(void* state, double value) {
    return createObjectType(state, value, "java/lang/Double", "(D)V");
}

extern "C" void *createStringObject(void* state, char *value, int32_t size) {
    auto handler = (MapJavaUdfOperatorHandler*) state;
    return handler->getEnvironment()->NewString(reinterpret_cast<const jchar*>(value), size);
}

template <typename T>
T getObjectTypeValue(void *state, void *object, std::string className, std::string getterName, std::string getterSignature) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler);
    auto mid = handler->getEnvironment()->GetMethodID(clazz, getterName.c_str(), getterSignature.c_str());
    jniErrorCheck(handler);
    T value;
     if (std::is_same<T, bool>::value){
        value = handler->getEnvironment()->CallBooleanMethod((jobject) object, mid);
    } else if (std::is_same<T, float>::value){
        value = handler->getEnvironment()->CallFloatMethod((jobject) object, mid);
    } else if (std::is_same<T, int32_t>::value) {
        value = handler->getEnvironment()->CallIntMethod((jobject) object, mid);
    } else if (std::is_same<T, int64_t>::value) {
         value = handler->getEnvironment()->CallLongMethod((jobject) object, mid);
    } else if (std::is_same<T, int16_t>::value) {
         value = handler->getEnvironment()->CallShortMethod((jobject) object, mid);
    } else if (std::is_same<T, double>::value) {
         value = handler->getEnvironment()->CallDoubleMethod((jobject) object, mid);
    }
    jniErrorCheck(handler);
    return value;
}

extern "C" bool getBooleanObjectValue(void* state, void *object) {
    return getObjectTypeValue<bool>(state, object, "java/lang/Boolean", "booleanValue", "()Z");
}

extern "C" float getFloatObjectValue(void* state, void *object) {
    return getObjectTypeValue<float>(state, object, "java/lang/Float", "floatValue", "()F");
}

extern "C" int32_t getIntegerObjectValue(void* state, void *object) {
    return getObjectTypeValue<int32_t>(state, object, "java/lang/Integer", "intValue", "()I");
}

extern "C" int64_t getLongObjectValue(void* state, void *object) {
    return getObjectTypeValue<int64_t>(state, object, "java/lang/Long", "longValue", "()J");
}

extern "C" int16_t getShortObjectValue(void* state, void *object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Short", "shortValue", "()S");
}

extern "C" double getDoubleObjectValue(void* state, void *object) {
    return getObjectTypeValue<double>(state, object, "java/lang/Double", "doubleValue", "()D");
}

extern "C" char *getStringObjectValue(void* state, void *object) {
    auto handler = (MapJavaUdfOperatorHandler*) state;
    auto size = handler->getEnvironment()->GetStringLength((jstring) object);
    return (char*) handler->getEnvironment()->GetStringChars((jstring) object, nullptr);
}

template <typename T>
T getField(void *state, void *classPtr, void *objectPtr, int fieldIndex, std::string signature){
    auto handler = (MapJavaUdfOperatorHandler*) state;

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler);
    T value;
    if (std::is_same<T, bool>::value){
        value = (T) handler->getEnvironment()->GetBooleanField(pojo, id);
    } else if (std::is_same<T, float>::value){
        value = (T) handler->getEnvironment()->GetFloatField(pojo, id);
    } else if (std::is_same<T, int32_t>::value) {
        value = (T) handler->getEnvironment()->GetIntField(pojo, id);
    } else if (std::is_same<T, int64_t>::value) {
        value = (T) handler->getEnvironment()->GetLongField(pojo, id);
    } else if (std::is_same<T, int16_t>::value) {
        value = (T) handler->getEnvironment()->GetShortField(pojo, id);
    } else if (std::is_same<T, double>::value) {
        value = (T) handler->getEnvironment()->GetDoubleField(pojo, id);
    }
    jniErrorCheck(handler);
    return value;
}

extern "C" bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<bool>(state, classPtr, objectPtr, fieldIndex, "Z");
}

extern "C" float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<float>(state, classPtr, objectPtr, fieldIndex, "F");
}

extern "C" int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int32_t>(state, classPtr, objectPtr, fieldIndex, "I");
}

extern "C" int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int64_t>(state, classPtr, objectPtr, fieldIndex, "J");
}

extern "C" int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int16_t>(state, classPtr, objectPtr, fieldIndex, "S");
}

extern "C" double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<double>(state, classPtr, objectPtr, fieldIndex, "D");
}

template <typename T>
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler);
    if (std::is_same<T, bool>::value){
        handler->getEnvironment()->SetBooleanField(pojo, id, (jboolean) value);
    } else if (std::is_same<T, float>::value){
        handler->getEnvironment()->SetFloatField(pojo, id, (jfloat) value);
    } else if (std::is_same<T, int32_t>::value) {
        handler->getEnvironment()->SetIntField(pojo, id, (jint) value);
    } else if (std::is_same<T, int64_t>::value) {
        handler->getEnvironment()->SetLongField(pojo, id, (jlong) value);
    } else if (std::is_same<T, int16_t>::value) {
        handler->getEnvironment()->SetShortField(pojo, id, (jshort) value);
    } else if (std::is_same<T, double>::value) {
        handler->getEnvironment()->SetDoubleField(pojo, id, (jdouble) value);
    }
    jniErrorCheck(handler);
}

extern "C" void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "I");
}

extern "C" void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "J");
}

extern "C" void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "S");
}

extern "C" void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "F");
}

extern "C" void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Z");
}

extern "C" void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "D");
}

extern "C" void* executeUdf(void* state, void* pojoObjectPtr) {
    auto handler = (MapJavaUdfOperatorHandler*) state;

    // Find class implementing the map udf
    jclass c1 = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
    jniErrorCheck(handler);

    // Build function signature of map function
    std::string sig = "(L" + handler->getInputClassName() + ";)L" + handler->getOutputClassName() + ";";

    // Find udf function
    jmethodID mid = handler->getEnvironment()->GetMethodID(c1, handler->getMethodName().c_str(), sig.c_str());
    jniErrorCheck(handler);

    jobject udf_result;
    // The map udf class will be either loaded from a serialized instance or allocated using class information
    if (!handler->getSerializedInstance().empty()) {
        // Load instance if defined
        jobject instance = deserializeInstance(state);

        // Call udf function
        udf_result = handler->getEnvironment()->CallObjectMethod(instance, mid, pojoObjectPtr);
        jniErrorCheck(handler);
    } else {
        // Create instance object using class information
        jclass clazz = handler->getEnvironment()->FindClass(handler->getClassName().c_str());
        jniErrorCheck(handler);

        // Here we assume the default constructor is available
        auto constr = handler->getEnvironment()->GetMethodID(clazz,"<init>", "()V");
        jobject instance = handler->getEnvironment()->NewObject(clazz, constr);
        jniErrorCheck(handler);

        // Call udf function
        udf_result = handler->getEnvironment()->CallObjectMethod(instance, mid, pojoObjectPtr);
        jniErrorCheck(handler);
    }
    return udf_result;
}

void MapJavaUdf::execute(ExecutionContext& ctx, Record& record) const {
    auto handler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    FunctionCall("startVM", startVM, handler);

    // Allocate java input class
    auto inputClassPtr = FunctionCall("findInputProxyClass", findInputProxyClass, handler);
    auto inputPojoPtr = FunctionCall("allocateObject", allocateObject, handler, inputClassPtr);

    // Loading record values into java input class
    // We derive the types of the values from the schema. The type can be complex of simple.
    // 1. Simple: tuples with one field represented through an object type (String, Integer, ..)
    // 2. Complex: plain old java object containing the multiple primitive types
    if (inputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        auto field = inputSchema->fields[0];
        auto fieldName = field->getName();

        if (field->getDataType()->isInteger()) {
            inputPojoPtr = FunctionCall<>("createIntegerObject", createIntegerObject, handler, record.read(fieldName).as<Int32>());
        } else if (field->getDataType()->isFloat()) {
            inputPojoPtr = FunctionCall<>("createFloatObject", createFloatObject, handler, record.read(fieldName).as<Float>());
        } else if (field->getDataType()->isBoolean()) {
            inputPojoPtr = FunctionCall<>("createBooleanObject", createBooleanObject, handler, record.read(fieldName).as<Boolean>());
        } else if (field->getDataType()->isText()) {
            NES_NOT_IMPLEMENTED();
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) inputSchema->fields.size(); i++) {
            auto field = inputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isInteger()) {
                FunctionCall<>("setIntegerField",
                               setIntegerField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Int32>());
            } else if (field->getDataType()->isFloat()) {
                FunctionCall<>("setFloatField",
                               setFloatField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Float>());
            } else if (field->getDataType()->isBoolean()) {
                FunctionCall<>("setBooleanField",
                               setBooleanField,
                               handler,
                               inputClassPtr,
                               inputPojoPtr,
                               Value<Int32>(i),
                               record.read(fieldName).as<Boolean>());
            } else if (field->getDataType()->isText()) {
                NES_NOT_IMPLEMENTED();
            }
        }
    }

    // Get output class and call udf
    auto outputClassPtr = FunctionCall<>("findOutputProxyClass", findOutputProxyClass, handler);
    auto outputPojoPtr = FunctionCall<>("executeUdf", executeUdf, handler, inputPojoPtr);

    // Create new record for result
    record = Record();

    // Reading result values from jvm into result record
    // Same differentiation as for input class above
    if (outputSchema->fields.size() == 1) {
        // 1. Simple, the input schema contains only one field
        auto field = outputSchema->fields[0];
        auto fieldName = field->getName();

        if (field->getDataType()->isInteger()) {
            Value<> val = FunctionCall<>("getIntegerObjectValue", getIntegerObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isFloat()) {
            Value<> val = FunctionCall<>("getFloatObjectValue", getFloatObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isBoolean()) {
            Value<> val = FunctionCall<>("getBooleanObjectValue", getBooleanObjectValue, handler, outputPojoPtr);
            record.write(fieldName, val);
        } else if (field->getDataType()->isText()) {
            NES_NOT_IMPLEMENTED();
        }
    } else {
        // 2. Complex, a plain old java object with multiple primitive types as map input
        for (int i = 0; i < (int) outputSchema->fields.size(); i++) {
            auto field = outputSchema->fields[i];
            auto fieldName = field->getName();

            if (field->getDataType()->isInteger()) {
                Value<> val =
                    FunctionCall<>("getIntegerField", getIntegerField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isFloat()) {
                Value<> val =
                    FunctionCall<>("getFloatField", getFloatField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isBoolean()) {
                Value<> val =
                    FunctionCall<>("getBooleanField", getBooleanField, handler, outputClassPtr, outputPojoPtr, Value<Int32>(i));
                record.write(fieldName, val);
            } else if (field->getDataType()->isText()) {
                NES_NOT_IMPLEMENTED();
            }
        }
    }

    FunctionCall<>("detachJVM", detachVM);

    // Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

void MapJavaUdf::terminate(ExecutionContext&) const {
    FunctionCall<>("destroyVM", destroyVM);
}

}// namespace NES::Runtime::Execution::Operators