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
#ifndef NES_RUNTIME_INCLUDE_UTIL_JNI_JNIUTILS_HPP_
#define NES_RUNTIME_INCLUDE_UTIL_JNI_JNIUTILS_HPP_
#include <Util/JNI/JNI.hpp>
#include <Util/SourceLocation.hpp>
#include <string_view>
#include <unordered_map>

/**
 * @brief This header provides basic functions that simplify the interaction with a JVM over JNI.
 */
namespace NES::jni {

using JavaSerializedInstance = std::vector<char>;
using JavaByteCode = std::vector<char>;
using JavaClassDefinition = std::pair<std::string, JavaByteCode>;
using JavaUDFByteCodeList = std::vector<JavaClassDefinition>;

/**
 * @brief Checks for a pending exception in the JNI environment and throws a runtime error if one is found.
 *
 * @param source_location source_location of callee
 */
void jniErrorCheck(const std::source_location location = std::source_location::current());

/**
 * Free a jvm object
 * @param object object to free
 */
void freeObject(jobject object);

/**
 * @brief Allocates a new object instance for to a specific class.
 * @param clazz
 * @return jobject
 */
jobject allocateObject(jclass clazz);

/**
 * @brief Finds a class with a given name.
 * @param clazzName
 * @return jclass
 */
jclass findClass(const std::string_view& clazzName);

/**
 * @brief Finds a method with a given name
 * @param methodName
 * @return jmethodID
 */
jmethodID getMethod(jclass clazz, const std::string_view& methodName, const std::string_view& signature);

/**
 * @brief Creates a java.lang.Boolean object for a given bool.
 * @param value
 * @return jobject
 */
jobject createBoolean(bool value);

/**
 * @brief Creates a java.lang.Float object for a given float.
 * @param value
 * @return jobject
 */
jobject createFloat(float value);

/**
 * @brief Creates a java.lang.Double object for a given double.
 * @param value
 * @return jobject
 */
jobject createDouble(double value);

/**
 * @brief Creates a java.lang.Integer object for a given int32_t.
 * @param value
 * @return jobject
 */
jobject createInteger(int32_t value);

/**
 * @brief Creates a java.lang.Long object for a given int64_t.
 * @param value
 * @return jobject
 */
jobject createLong(int64_t value);

/**
 * @brief Creates a java.lang.Short object for a given int16_t.
 * @param value
 * @return jobject
 */
jobject createShort(int16_t value);

/**
 * @brief Creates a java.lang.Byte object for a given int8_t.
 * @param value
 * @return jobject
 */
jobject createByte(int8_t value);

/**
 * @brief Creates a java.lang.String object for a given std::string_view.
 * @param value
 * @return jobject
 */
jstring createString(const std::string_view& value);

/**
 * @brief Extracts a bool from a java.lang.Boolean object
 * @param object
 * @return bool
 */
bool getBooleanValue(jobject object);

/**
 * @brief Extracts a float from a java.lang.Float object
 * @param object
 * @return float
 */
float getFloatValue(jobject object);

/**
 * @brief Extracts a double from a java.lang.Double object
 * @param object
 * @return double
 */
double getDoubleValue(jobject object);
/**
 * @brief Extracts a int32_t from a java.lang.Integer object
 * @param object
 * @return int32_t
 */
int32_t getIntegerValue(jobject object);

/**
 * @brief Extracts a int64_t from a java.lang.Long object
 * @param object
 * @return bool
 */
int64_t getLongValue(jobject object);

/**
 * @brief Extracts a int16_t from a java.lang.Short object
 * @param object
 * @return int16_t
 */
int16_t getShortValue(jobject object);

/**
 * @brief Extracts a int8_t from a java.lang.Byte object
 * @param object
 * @return int8_t
 */
int8_t getByteValue(jobject object);

/**
 * @brief Extracts a std::string from a java.lang.String object
 * @param object
 * @return std::string
 */
std::string getStringObjectValue(jstring object);

/**
 * @brief Converts a class name into the correct jni name
 * @param javaClassName
 * @return std::string
 */
const std::string convertToJNIName(const std::string& javaClassName);

}// namespace NES::jni

#endif  // NES_RUNTIME_INCLUDE_UTIL_JNI_JNIUTILS_HPP_
