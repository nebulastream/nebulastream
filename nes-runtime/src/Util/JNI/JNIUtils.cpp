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

#include <Util/JNI/JNI.hpp>
#include <Util/JNI/JNIUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <jni.h>
#include <string_view>

namespace NES::jni {

void jniErrorCheck(const std::source_location location) {
    auto env = getEnv();
    auto exception = env->ExceptionOccurred();
    if (exception) {
        // print exception
        jboolean isCopy = false;
        auto clazz = env->FindClass("java/lang/Object");
        auto toString = env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
        auto string = (jstring) env->CallObjectMethod(exception, toString);
        const char* utf = env->GetStringUTFChars(string, &isCopy);
        NES_THROW_RUNTIME_ERROR("An error occurred during a map java UDF execution in function "
                                << location.function_name() << " at " << location.file_name() << ":" << location.line() << ": "
                                << utf);
    }
}

jmethodID getMethod(jclass clazz, const std::string_view& methodName, const std::string_view& signature) {
    auto method = getEnv()->GetMethodID(clazz, methodName.data(), signature.data());
    jniErrorCheck();
    return method;
}
jobject allocateObject(jclass clazz) { return getEnv()->AllocObject(clazz); }

void freeObject(jobject object) {
    auto env = getEnv();
    switch (env->GetObjectRefType(object)) {
        case JNIInvalidRefType: NES_THROW_RUNTIME_ERROR("Can't delete invalid ref!");
        case JNILocalRefType: env->DeleteLocalRef(object); break;
        case JNIGlobalRefType: env->DeleteGlobalRef(object); break;
        case JNIWeakGlobalRefType: env->DeleteGlobalRef(object); break;
    }
}

jclass findClass(const std::string_view& className) {
    jclass clazz = getEnv()->FindClass(className.data());
    jniErrorCheck();
    return clazz;
}

jclass findClass(const std::string_view& className, const jobject classLoader) {
    if (!classLoader) {
        return findClass(convertToJNIName(std::string(className)));
    }
    auto loaderClazz = findClass("stream/nebula/UDFClassLoader");
    auto findClassMethod = getMethod(loaderClazz, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;");
    auto javaString = createString(className);
    jniErrorCheck();
    auto clazz = jni::getEnv()->CallObjectMethod(classLoader, findClassMethod, javaString);
    jniErrorCheck();
    freeObject(javaString);
    return static_cast<jclass>(clazz);
}

template<typename T>
jobject createObjectType(T value, const std::string_view& className, const std::string_view& constructorSignature) {
    auto env = getEnv();
    auto clazz = findClass(className);
    auto mid = getMethod(clazz, "<init>", constructorSignature);
    auto object = env->NewObject(clazz, mid, value);
    jniErrorCheck();
    object = env->NewGlobalRef(object);
    NES_ASSERT(object != NULL, "the new global reference should not be null");
    return object;
}

jobject createBoolean(bool value) { return createObjectType(value, "java/lang/Boolean", "(Z)V"); }

jobject createFloat(float value) { return createObjectType(value, "java/lang/Float", "(F)V"); }

jobject createDouble(double value) { return createObjectType(value, "java/lang/Double", "(D)V"); }

jobject createInteger(int32_t value) { return createObjectType(value, "java/lang/Integer", "(I)V"); }

jobject createLong(int64_t value) { return createObjectType(value, "java/lang/Long", "(J)V"); }

jobject createShort(int16_t value) { return createObjectType(value, "java/lang/Short", "(S)V"); }

jobject createByte(int8_t value) { return createObjectType(value, "java/lang/Byte", "(B)V"); }

jstring createString(const std::string_view& value) {
    auto env = getEnv();
    jstring string = env->NewStringUTF(value.data());
    return static_cast<jstring>(getEnv()->NewGlobalRef((jobject) string));
}

template<typename T>
T getObjectTypeValue(jobject object,
                     const std::string_view& className,
                     const std::string_view& getterName,
                     const std::string_view& getterSignature) {
    NES_ASSERT(getEnv()->GetObjectRefType(object) != JNIInvalidRefType, "object ref is invalid");
    auto env = getEnv();
    auto clazz = findClass(className);
    auto mid = getMethod(clazz, getterName, getterSignature);
    T value;
    if constexpr (std::is_same<T, bool>::value) {
        value = env->CallBooleanMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, float>::value) {
        value = env->CallFloatMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, double>::value) {
        value = env->CallDoubleMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int32_t>::value) {
        value = env->CallIntMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int64_t>::value) {
        value = env->CallLongMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int16_t>::value) {
        value = env->CallShortMethod((jobject) object, mid);
    } else if constexpr (std::is_same<T, int8_t>::value) {
        value = env->CallByteMethod((jobject) object, mid);
    } else {
        NES_THROW_RUNTIME_ERROR("Unsupported type: " + std::string(typeid(T).name()));
    }
    jniErrorCheck();
    return value;
}
bool getBooleanValue(jobject object) { return getObjectTypeValue<bool>(object, "java/lang/Boolean", "booleanValue", "()Z"); }

float getFloatValue(jobject object) { return getObjectTypeValue<float>(object, "java/lang/Float", "floatValue", "()F"); }
double getDoubleValue(jobject object) { return getObjectTypeValue<double>(object, "java/lang/Double", "doubleValue", "()D"); }

int32_t getIntegerValue(jobject object) { return getObjectTypeValue<int32_t>(object, "java/lang/Integer", "intValue", "()I"); }

int64_t getLongValue(jobject object) { return getObjectTypeValue<int64_t>(object, "java/lang/Long", "longValue", "()J"); }

int16_t getShortValue(jobject object) { return getObjectTypeValue<int16_t>(object, "java/lang/Short", "shortValue", "()S"); }

int8_t getByteValue(jobject object) { return getObjectTypeValue<int8_t>(object, "java/lang/Byte", "byteValue", "()B"); }

std::string getStringObjectValue(jstring object) {
    NES_ASSERT2_FMT(object != nullptr, "object should not be null");
    auto size = jni::getEnv()->GetStringUTFLength((jstring) object);
    auto sourceText = jni::getEnv()->GetStringUTFChars((jstring) object, nullptr);
    return std::string(sourceText, size);
}

const std::string convertToJNIName(const std::string& javaClassName) {
    std::string result = javaClassName;
    std::replace(result.begin(), result.end(), '.', '/');
    return result;
}

jni::jobject deserializeInstance(const jni::JavaSerializedInstance& serializedInstance) {
    auto env = getEnv();
    const auto length = serializedInstance.size();
    const auto data = reinterpret_cast<const jbyte*>(serializedInstance.data());
    const auto byteArray = env->NewByteArray(length);
    jniErrorCheck();
    env->SetByteArrayRegion(byteArray, 0, length, data);
    jniErrorCheck();

    // Deserialize the instance using a Java helper method.
    const auto clazz = env->FindClass("stream/nebula/MapJavaUdfUtils");
    jniErrorCheck();
    // TODO #3738: we can probably cache the method id for all functions in e.g. the operator handler to improve performance
    const auto mid = env->GetMethodID(clazz, "deserialize", "([B)Ljava/lang/Object;");
    jniErrorCheck();
    const auto obj = env->CallStaticObjectMethod(clazz, mid, byteArray);
    jniErrorCheck();
    // Release the array.
    env->DeleteLocalRef(byteArray);
    jniErrorCheck();
    return obj;
}

void loadClassesFromByteList(const jobject classLoader, const jni::JavaUDFByteCodeList& byteCodeList) {
    for (auto& [className, byteCode] : byteCodeList) {
        auto env = getEnv();
        jbyteArray jData = env->NewByteArray(byteCode.size());
        jniErrorCheck();
        jbyte* jCode = env->GetByteArrayElements(jData, nullptr);
        jniErrorCheck();
        std::memcpy(jCode, byteCode.data(), byteCode.size());// copy the byte array into the JVM byte array
        const auto jniName = convertToJNIName(className);
        NES_DEBUG("Injecting Java class into JVM: {}", className);
        env->DefineClass(jniName.c_str(), classLoader, jCode, (jint) byteCode.size());
        jniErrorCheck();
        env->ReleaseByteArrayElements(jData, jCode, JNI_ABORT);
        jniErrorCheck();
    }
}

}// namespace NES::jni