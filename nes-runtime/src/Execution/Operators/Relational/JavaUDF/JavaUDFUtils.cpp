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

#include <API/Schema.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void freeObject(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    handler->getEnvironment()->DeleteLocalRef((jobject) object);
}

inline bool dirExists(const std::string& path) { return std::filesystem::exists(path.c_str()); }

void loadClassesFromByteList(void* state, const std::unordered_map<std::string, std::vector<char>>& byteCodeList) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    for (auto& [className, byteCode] : byteCodeList) {
        jbyteArray jData = handler->getEnvironment()->NewByteArray(byteCode.size());
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
        jbyte* jCode = handler->getEnvironment()->GetByteArrayElements(jData, nullptr);
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
        std::memcpy(jCode, byteCode.data(), byteCode.size());// copy the byte array into the JVM byte array
        handler->getEnvironment()->DefineClass(className.c_str(), nullptr, jCode, (jint) byteCode.size());
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
        handler->getEnvironment()->ReleaseByteArrayElements(jData, jCode, JNI_ABORT);
        jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    }
}

jobject deserializeInstance(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    auto env = handler->getEnvironment();

    // Create an array from the serialized instance.
    auto length = handler->getSerializedInstance().size();
    auto data = reinterpret_cast<const jbyte*>(handler->getSerializedInstance().data());
    auto byteArray = env->NewByteArray(length);
    jniErrorCheck(env, __func__, __LINE__);
    env->SetByteArrayRegion(byteArray, 0, length, data);
    jniErrorCheck(env, __func__, __LINE__);

    // Deserialize the instance using a Java helper method.
    auto clazz = env->FindClass("MapJavaUdfUtils");
    jniErrorCheck(env, __func__, __LINE__);
    // TODO: we can probably cache the method id for all functions in e.g. the operator handler to improve performance
    auto mid = env->GetMethodID(clazz, "deserialize", "([B)Ljava/lang/Object;");
    jniErrorCheck(env, __func__, __LINE__);
    auto obj = env->CallStaticObjectMethod(clazz, mid, byteArray);
    jniErrorCheck(env, __func__, __LINE__);

    // Release the array.
    env->DeleteLocalRef(byteArray);
    jniErrorCheck(env, __func__, __LINE__);
    return obj;
}

void startOrAttachVMWithJarFile(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    // Sanity check javaPath
    auto javaPath = handler->getJavaPath().value();
    if (!dirExists(javaPath)) {
        NES_FATAL_ERROR("jarPath:" << javaPath << " not valid!");
        exit(EXIT_FAILURE);
    }

    JavaVMInitArgs args{};
    std::vector<std::string> opt{"-verbose:jni", "-verbose:class", "-Djava.class.path=" + javaPath};
    std::vector<JavaVMOption> options;
    for (const auto& s : opt) {
        options.push_back(JavaVMOption{.optionString = const_cast<char*>(s.c_str())});
    }
    args.version = JNI_VERSION_1_2;
    args.ignoreUnrecognized = false;
    args.options = options.data();
    args.nOptions = std::size(options);

    auto env = handler->getEnvironment();
    JVMContext::instance().createOrAttachToJVM(&env, args);
    handler->setEnvironment(env);
}

void startOrAttachVMWithByteList(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    std::string utilsPath = std::string(JAVA_UDF_UTILS);
    JavaVMInitArgs args{};
    std::vector<std::string> opt{"-verbose:jni", "-verbose:class", "-Djava.class.path=" + utilsPath};
    std::vector<JavaVMOption> options;
    for (const auto& s : opt) {
        options.push_back(JavaVMOption{.optionString = const_cast<char*>(s.c_str())});
    }
    args.version = JNI_VERSION_1_2;
    args.ignoreUnrecognized = false;
    args.options = options.data();
    args.nOptions = std::size(options);

    bool created = JVMContext::instance().isJVMCreated();
    auto env = handler->getEnvironment();
    JVMContext::instance().createOrAttachToJVM(&env, args);
    handler->setEnvironment(env);
    if (!created) {
        loadClassesFromByteList(handler, handler->getByteCodeList());
    }
}

void startOrAttachVM(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    if (handler->getJavaPath().has_value()) {
        // Get java classes using jar files
        startOrAttachVMWithJarFile(state);
    } else {
        // Get java classes using byte list
        startOrAttachVMWithByteList(state);
    }
}

void detachVM() { JVMContext::instance().detachFromJVM(); }

void destroyVM() { JVMContext::instance().destroyJVM(); }

void* findInputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getInputClassName().c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return clazz;
}

void* findOutputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getOutputClassName().c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return clazz;
}

void* allocateObject(void* state, void* classPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto clazz = (jclass) classPtr;
    jobject obj = handler->getEnvironment()->AllocObject(clazz);
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return obj;
}

template<typename T>
void* createObjectType(void* state, T value, std::string className, std::string constructorSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "<init>", constructorSignature.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    auto object = handler->getEnvironment()->NewObject(clazz, mid, value);
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return object;
}

void* createBooleanObject(void* state, bool value) { return createObjectType(state, value, "java/lang/Boolean", "(Z)V"); }

void* createFloatObject(void* state, float value) { return createObjectType(state, value, "java/lang/Float", "(F)V"); }

void* createDoubleObject(void* state, double value) { return createObjectType(state, value, "java/lang/Double", "(D)V"); }

void* createIntegerObject(void* state, int32_t value) { return createObjectType(state, value, "java/lang/Integer", "(I)V"); }

void* createLongObject(void* state, int64_t value) { return createObjectType(state, value, "java/lang/Long", "(J)V"); }

void* createShortObject(void* state, int16_t value) { return createObjectType(state, value, "java/lang/Short", "(S)V"); }

void* createByteObject(void* state, int8_t value) { return createObjectType(state, value, "java/lang/Byte", "(B)V"); }

void* createStringObject(void* state, TextValue* value) {
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);
    return handler->getEnvironment()->NewStringUTF(value->c_str());
}

template<typename T>
T getObjectTypeValue(void* state, void* object, std::string className, std::string getterName, std::string getterSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    auto mid = handler->getEnvironment()->GetMethodID(clazz, getterName.c_str(), getterSignature.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = handler->getEnvironment()->CallBooleanMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, float>::value) {
        value = handler->getEnvironment()->CallFloatMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, double>::value) {
        value = handler->getEnvironment()->CallDoubleMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = handler->getEnvironment()->CallIntMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = handler->getEnvironment()->CallLongMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = handler->getEnvironment()->CallShortMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = handler->getEnvironment()->CallByteMethod((jobject) object, mid);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return value;
}

bool getBooleanObjectValue(void* state, void* object) {
    return getObjectTypeValue<bool>(state, object, "java/lang/Boolean", "booleanValue", "()Z");
}

float getFloatObjectValue(void* state, void* object) {
    return getObjectTypeValue<float>(state, object, "java/lang/Float", "floatValue", "()F");
}

double getDoubleObjectValue(void* state, void* object) {
    return getObjectTypeValue<double>(state, object, "java/lang/Double", "doubleValue", "()D");
}

int32_t getIntegerObjectValue(void* state, void* object) {
    return getObjectTypeValue<int32_t>(state, object, "java/lang/Integer", "intValue", "()I");
}

int64_t getLongObjectValue(void* state, void* object) {
    return getObjectTypeValue<int64_t>(state, object, "java/lang/Long", "longValue", "()J");
}

int16_t getShortObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Short", "shortValue", "()S");
}

int8_t getByteObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Byte", "byteValue", "()B");
}

TextValue* getStringObjectValue(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(object != nullptr, "object should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto size = handler->getEnvironment()->GetStringUTFLength((jstring) object);
    auto resultText = TextValue::create(size);
    auto sourceText = handler->getEnvironment()->GetStringUTFChars((jstring) object, nullptr);
    std::memcpy(resultText->str(), sourceText, size);
    return resultText;
}

template<typename T>
T getField(void* state, void* classPtr, void* objectPtr, int fieldIndex, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getUdfOutputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = (T) handler->getEnvironment()->GetBooleanField(pojo, id);
    } else if constexpr (std::is_same<T, float>::value) {
        value = (T) handler->getEnvironment()->GetFloatField(pojo, id);
    } else if constexpr (std::is_same<T, double>::value) {
        value = (T) handler->getEnvironment()->GetDoubleField(pojo, id);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = (T) handler->getEnvironment()->GetIntField(pojo, id);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = (T) handler->getEnvironment()->GetLongField(pojo, id);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = (T) handler->getEnvironment()->GetShortField(pojo, id);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = (T) handler->getEnvironment()->GetByteField(pojo, id);
    } else if constexpr (std::is_same<T, TextValue*>::value) {
        auto jstr = (jstring) handler->getEnvironment()->GetObjectField(pojo, id);
        auto size = handler->getEnvironment()->GetStringUTFLength((jstring) jstr);
        value = TextValue::create(size);
        auto resultText = handler->getEnvironment()->GetStringUTFChars(jstr, 0);
        std::memcpy(value->str(), resultText, size);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    return value;
}

bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<bool>(state, classPtr, objectPtr, fieldIndex, "Z");
}

float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<float>(state, classPtr, objectPtr, fieldIndex, "F");
}

double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<double>(state, classPtr, objectPtr, fieldIndex, "D");
}

int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int32_t>(state, classPtr, objectPtr, fieldIndex, "I");
}

int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int64_t>(state, classPtr, objectPtr, fieldIndex, "J");
}

int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int16_t>(state, classPtr, objectPtr, fieldIndex, "S");
}

int8_t getByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int8_t>(state, classPtr, objectPtr, fieldIndex, "B");
}

TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<TextValue*>(state, classPtr, objectPtr, fieldIndex, "Ljava/lang/String;");
}

template<typename T>
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<JavaUDFOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getUdfInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
    if constexpr (std::is_same<T, bool>::value) {
        handler->getEnvironment()->SetBooleanField(pojo, id, (jboolean) value);
    } else if constexpr (std::is_same<T, float>::value) {
        handler->getEnvironment()->SetFloatField(pojo, id, (jfloat) value);
    } else if constexpr (std::is_same<T, double>::value) {
        handler->getEnvironment()->SetDoubleField(pojo, id, (jdouble) value);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        handler->getEnvironment()->SetIntField(pojo, id, (jint) value);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        handler->getEnvironment()->SetLongField(pojo, id, (jlong) value);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        handler->getEnvironment()->SetShortField(pojo, id, (jshort) value);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        handler->getEnvironment()->SetByteField(pojo, id, (jbyte) value);
    } else if constexpr (std::is_same<T, const TextValue*>::value) {
        const TextValue* sourceString = value;
        jstring string = handler->getEnvironment()->NewStringUTF(sourceString->c_str());
        handler->getEnvironment()->SetObjectField(pojo, id, (jstring) string);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck(handler->getEnvironment(), __func__, __LINE__);
}

void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Z");
}

void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "F");
}

void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "D");
}

void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "I");
}

void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "J");
}

void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "S");
}

void setByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int8_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "B");
}

void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const TextValue* value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Ljava/lang/String;");
}

}// namespace NES::Runtime::Execution::Operators
#endif// ENABLE_JNI