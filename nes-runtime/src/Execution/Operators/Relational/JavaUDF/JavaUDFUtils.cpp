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
#include <Execution/Operators/Relational/JavaUDF/JavaUDFUtils.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUdfOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <jni.h>
#include <utility>

#include <API/Schema.hpp>
#include <API/AttributeField.hpp>
#include <jni.h>
#include <Util/Logger/Logger.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
// TODO Change this to general class
#include <Execution/Operators/Relational/JavaUDF/MapJavaUdfOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
// TODO check if other version needed
#include <filesystem>

namespace NES::Runtime::Execution::Operators {

/**
 * free a jvm object
 * @param state operator handler state
 * @param object object to free
 */
void freeObject(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    handler->getEnvironment()->DeleteLocalRef((jobject) object);
}


/**
 * This function is used for JNI error handling.
 * @param env jni environment
 * @param location location of the error. Leave default to use the location of the caller.
 */
inline void jniErrorCheck(JNIEnv* env, const std::source_location& location) {
    auto exception = env->ExceptionOccurred();
    if (exception) {
        // print exception
        jboolean isCopy = false;
        auto clazz = env->FindClass("java/lang/Object");
        auto toString = env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
        auto string = (jstring) env->CallObjectMethod(exception, toString);
        const char* utf = env->GetStringUTFChars(string, &isCopy);
        NES_THROW_RUNTIME_ERROR("An error occurred during a map java UDF execution in function "
                                << location.function_name() << " at line " << location.line() << ": " << utf);
    }
}

/**
 * Returns if directory of path exists.
 * @param name path to check
 * @return bool if directory exists
 */
inline bool dirExists(const std::string& path) { return std::filesystem::exists(path.c_str()); }

/**
 * loads clases from the byteCodeList into the JVM
 * @param state operator handler state
 * @param byteCodeList byte code list
 */
void loadClassesFromByteList(void* state, const std::unordered_map<std::string, std::vector<char>>& byteCodeList) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    for (auto& [className, byteCode] : byteCodeList) {
        jbyteArray jData = handler->getEnvironment()->NewByteArray(byteCode.size());
        jniErrorCheck(handler->getEnvironment());
        jbyte* jCode = handler->getEnvironment()->GetByteArrayElements(jData, nullptr);
        jniErrorCheck(handler->getEnvironment());
        std::memcpy(jCode, byteCode.data(), byteCode.size());// copy the byte array into the JVM byte array
        handler->getEnvironment()->DefineClass(className.c_str(), nullptr, jCode, (jint) byteCode.size());
        jniErrorCheck(handler->getEnvironment());
        handler->getEnvironment()->ReleaseByteArrayElements(jData, jCode, JNI_ABORT);
        jniErrorCheck(handler->getEnvironment());
    }
}

/**
 * Deserializes the given instance
 * @param state operator handler state
 */
jobject deserializeInstance(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    // use deserializer given in java utils file
    void* object = (void*) handler->getSerializedInstance().data();
    auto clazz = handler->getEnvironment()->FindClass("MapJavaUdfUtils");
    jniErrorCheck(handler->getEnvironment());
    // TODO: we can probably cache the method id for all functions in e.g. the operator handler to improve performance
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "deserialize", "(Ljava/nio/ByteBuffer;)Ljava/lang/Object;");
    jniErrorCheck(handler->getEnvironment());
    auto obj = handler->getEnvironment()->CallStaticObjectMethod(clazz, mid, object);
    jniErrorCheck(handler->getEnvironment());
    return obj;
}

/**
 * Start the java vm and load the classes given in the javaPath
 * @param state operator handler state
 */
void startOrAttachVMWithJarFile(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

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

/**
 * Start the java vm and load the classes given in the byteCodeList
 * @param state operator handler state
 */
void startOrAttachVMWithByteList(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

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

/**
 * Wrapper for starting or attaching to the java vm.
 * The java classes will be either loaded from the given jar file or from the given byte code list.
 * When no java path is given, the byte code list is used.
 * @param state operator handler state
 */
void startOrAttachVM(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    if (handler->getJavaPath().has_value()) {
        // Get java classes using jar files
        startOrAttachVMWithJarFile(state);
    } else {
        // Get java classes using byte list
        startOrAttachVMWithByteList(state);
    }
}

/**
 * Detach the current thread from the JVM.
 * This is needed to avoid memory leaks.
 */
void detachVM() { JVMContext::instance().detachFromJVM(); }

/**
 * Unloads the java VM.
 * This is needed to avoid memory leaks.
 */
void destroyVM() { JVMContext::instance().destroyJVM(); }

/**
 * Finds the input class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @return jclass input class object pointer
 */
void* findInputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getInputClassName().c_str());
    jniErrorCheck(handler->getEnvironment());
    return clazz;
}

/**
 * Finds the output class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @class jclass output class object pointer
 */
void* findOutputClass(void* state) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    jclass clazz = handler->getEnvironment()->FindClass(handler->getOutputClassName().c_str());
    jniErrorCheck(handler->getEnvironment());
    return clazz;
}

/**
 * Allocates a new instance of the given class.
 * @param state operator handler state
 * @param classPtr class to allocate
 * @return jobject instance of the given class
 */
void* allocateObject(void* state, void* classPtr) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = (jclass) classPtr;
    jobject obj = handler->getEnvironment()->AllocObject(clazz);
    jniErrorCheck(handler->getEnvironment());
    return obj;
}

/**
 * Creates a new instance of a class and sets its value in the constructor.
 * @tparam T type of the class
 * @param state operator handler state
 * @param value value to set
 * @param className name of the class
 * @param constructorSignature signature of the constructor
 * @return jobject instance of the given class
 */
template<typename T>
void* createObjectType(void* state, T value, std::string className, std::string constructorSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto mid = handler->getEnvironment()->GetMethodID(clazz, "<init>", constructorSignature.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto object = handler->getEnvironment()->NewObject(clazz, mid, value);
    jniErrorCheck(handler->getEnvironment());
    return object;
}

/**
 * Creates a new boolean object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createBooleanObject(void* state, bool value) {
    return createObjectType(state, value, "java/lang/Boolean", "(Z)V");
}

/**
 * Creates a new float object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createFloatObject(void* state, float value) { return createObjectType(state, value, "java/lang/Float", "(F)V"); }

/**
 * Creates a new double object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createDoubleObject(void* state, double value) {
    return createObjectType(state, value, "java/lang/Double", "(D)V");
}

/**
 * Creates a new int object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createIntegerObject(void* state, int32_t value) {
    return createObjectType(state, value, "java/lang/Integer", "(I)V");
}

/**
 * Creates a new long object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createLongObject(void* state, int64_t value) { return createObjectType(state, value, "java/lang/Long", "(J)V"); }

/**
 * Creates a new short object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createShortObject(void* state, int16_t value) {
    return createObjectType(state, value, "java/lang/Short", "(S)V");
}

/**
 * Creates a new java byte object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createByteObject(void* state, int8_t value) { return createObjectType(state, value, "java/lang/Byte", "(B)V"); }

/**
 * Creates a new string object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createStringObject(void* state, TextValue* value) {
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);
    return handler->getEnvironment()->NewStringUTF(value->c_str());
}

/**
 * Get the value of an object of type boolean, float, double, int, long, short, byte or string.
 * @tparam T type of value
 * @param state operator handler state
 * @param object object to get the value from
 * @param className class name of the object
 * @param getterName getter function name of the value
 * @param getterSignature getter function signature of the value
 * @return T value of the field
 */
template<typename T>
T getObjectTypeValue(void* state, void* object, std::string className, std::string getterName, std::string getterSignature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto clazz = handler->getEnvironment()->FindClass(className.c_str());
    jniErrorCheck(handler->getEnvironment());
    auto mid = handler->getEnvironment()->GetMethodID(clazz, getterName.c_str(), getterSignature.c_str());
    jniErrorCheck(handler->getEnvironment());
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
    jniErrorCheck(handler->getEnvironment());
    return value;
}

/**
 * Get boolean value of a bool object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return bool value of the field
 */
bool getBooleanObjectValue(void* state, void* object) {
    return getObjectTypeValue<bool>(state, object, "java/lang/Boolean", "booleanValue", "()Z");
}

/**
 * Get float value of a flaot object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return float value of the field
 */
float getFloatObjectValue(void* state, void* object) {
    return getObjectTypeValue<float>(state, object, "java/lang/Float", "floatValue", "()F");
}

/**
 * Get double value of a double object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return double value of the field
 */
double getDoubleObjectValue(void* state, void* object) {
    return getObjectTypeValue<double>(state, object, "java/lang/Double", "doubleValue", "()D");
}

/**
 * Get int value of a integer object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return int value of the field
 */
int32_t getIntegerObjectValue(void* state, void* object) {
    return getObjectTypeValue<int32_t>(state, object, "java/lang/Integer", "intValue", "()I");
}

/**
 * Get long value of a long object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return long value of the field
 */
int64_t getLongObjectValue(void* state, void* object) {
    return getObjectTypeValue<int64_t>(state, object, "java/lang/Long", "longValue", "()J");
}

/**
 * Get short value of a short object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return short value of the field
 */
int16_t getShortObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Short", "shortValue", "()S");
}

/**
 * Get byte value of
 * @param state operator handler state
 * @param object object to get the field from
 * @return byte value of the field
 */
int8_t getByteObjectValue(void* state, void* object) {
    return getObjectTypeValue<int16_t>(state, object, "java/lang/Byte", "byteValue", "()B");
}

/**
 * Get string value of a string object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return TextValue value of the field
 */
TextValue* getStringObjectValue(void* state, void* object) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(object != nullptr, "object should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto size = handler->getEnvironment()->GetStringUTFLength((jstring) object);
    auto resultText = TextValue::create(size);
    auto sourceText = handler->getEnvironment()->GetStringUTFChars((jstring) object, nullptr);
    std::memcpy(resultText->str(), sourceText, size);
    return resultText;
}

/**
 * Get the value of a field of an object.
 * @tparam T type of the field
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @param signature signature of the field
 * @return T value of the field
 */
template<typename T>
T getField(void* state, void* classPtr, void* objectPtr, int fieldIndex, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment());
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
    jniErrorCheck(handler->getEnvironment());
    return value;
}

/**
 * Get the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return bool value of the field
 */
bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<bool>(state, classPtr, objectPtr, fieldIndex, "Z");
}

/**
 * Get the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return float value of the field
 */
float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<float>(state, classPtr, objectPtr, fieldIndex, "F");
}

/**
 * Get the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return double value of the field
 */
double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<double>(state, classPtr, objectPtr, fieldIndex, "D");
}

/**
 * Get the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int32_t value of the field
 */
int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int32_t>(state, classPtr, objectPtr, fieldIndex, "I");
}

/**
 * Get the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int64_t value of the field
 */
int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int64_t>(state, classPtr, objectPtr, fieldIndex, "J");
}

/**
 * Get the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int16_t value of the field
 */
int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int16_t>(state, classPtr, objectPtr, fieldIndex, "S");
}

/**
 * Get the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int8_t value of the field
 */
int8_t getByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<int8_t>(state, classPtr, objectPtr, fieldIndex, "B");
}

/**
 * Get the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return TextValue* value of the field
 */
TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex) {
    return getField<TextValue*>(state, classPtr, objectPtr, fieldIndex, "Ljava/lang/String;");
}

/**
 * Set the value of a field of an object.
 * @tparam T type of the field
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 * @param signature signature of the field
 */
template<typename T>
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature) {
    NES_ASSERT2_FMT(state != nullptr, "op handler context should not be null");
    NES_ASSERT2_FMT(classPtr != nullptr, "classPtr should not be null");
    NES_ASSERT2_FMT(objectPtr != nullptr, "objectPtr should not be null");
    auto handler = static_cast<MapJavaUdfOperatorHandler*>(state);

    auto pojoClass = (jclass) classPtr;
    auto pojo = (jobject) objectPtr;
    std::string fieldName = handler->getInputSchema()->fields[fieldIndex]->getName();
    jfieldID id = handler->getEnvironment()->GetFieldID(pojoClass, fieldName.c_str(), signature.c_str());
    jniErrorCheck(handler->getEnvironment());
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
    jniErrorCheck(handler->getEnvironment());
}

/**
 * Set the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Z");
}

/**
 * Set the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "F");
}

/**
 * Set the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "D");
}

/**
 * Set the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "I");
}

/**
 * Set the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "J");
}

/**
 * Set the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "S");
}

/**
 * Set the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int8_t value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "B");
}

/**
 * Set the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const TextValue* value) {
    return setField(state, classPtr, objectPtr, fieldIndex, value, "Ljava/lang/String;");
}

} // namespace NES::Runtime::Execution::Operators
#endif // ENABLE_JNI