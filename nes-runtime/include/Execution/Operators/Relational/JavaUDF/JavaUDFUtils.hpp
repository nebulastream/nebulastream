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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFUTILS_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFUTILS_HPP_

#ifdef ENABLE_JNI

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Relational/JavaUDF/JVMContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Util/SourceLocation.hpp>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Checks for a pending exception in the JNI environment and throws a runtime error if one is found.
 *
 * @param env JNI environment
 * @param func_name name of the function where the error occurred: should be __func__
 * @param line_number line number where the error occurred: should be __LINE__
 */
inline void jniErrorCheck(JNIEnv* env, const char* func_name, int line_number) {
    auto exception = env->ExceptionOccurred();
    if (exception) {
        // print exception
        jboolean isCopy = false;
        auto clazz = env->FindClass("java/lang/Object");
        auto toString = env->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
        auto string = (jstring) env->CallObjectMethod(exception, toString);
        const char* utf = env->GetStringUTFChars(string, &isCopy);
        NES_THROW_RUNTIME_ERROR("An error occurred during a map java UDF execution in function " << func_name << " at line "
                                                                                                 << line_number << ": " << utf);
    }
}

/**
 * free a jvm object
 * @param state operator handler state
 * @param object object to free
 */
void freeObject(void* state, void* object);

/**
 * Returns if directory of path exists.
 * @param name path to check
 * @return bool if directory exists
 */
inline bool dirExists(const std::string& path);

/**
 * loads clases from the byteCodeList into the JVM
 * @param state operator handler state
 * @param byteCodeList byte code list
 */
void loadClassesFromByteList(void* state, const std::unordered_map<std::string, std::vector<char>>& byteCodeList);

/**
 * Deserializes the given instance
 * @param state operator handler state
 */
jobject deserializeInstance(void* state);

/**
 * Start the java vm and load the classes given in the javaPath
 * @param state operator handler state
 */
void startOrAttachVMWithJarFile(void* state);

/**
 * Start the java vm and load the classes given in the byteCodeList
 * @param state operator handler state
 */
void startOrAttachVMWithByteList(void* state);

/**
 * Wrapper for starting or attaching to the java vm.
 * The java classes will be either loaded from the given jar file or from the given byte code list.
 * When no java path is given, the byte code list is used.
 * @param state operator handler state
 */
void startOrAttachVM(void* state);

/**
 * Detach the current thread from the JVM.
 * This is needed to avoid memory leaks.
 */
void detachVM();

/**
 * Unloads the java VM.
 * This is needed to avoid memory leaks.
 */
void destroyVM();

/**
 * Finds the input class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @return jclass input class object pointer
 */
void* findInputClass(void* state);

/**
 * Finds the output class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @class jclass output class object pointer
 */
void* findOutputClass(void* state);

/**
 * Allocates a new instance of the given class.
 * @param state operator handler state
 * @param classPtr class to allocate
 * @return jobject instance of the given class
 */
void* allocateObject(void* state, void* classPtr);

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
void* createObjectType(void* state, T value, std::string className, std::string constructorSignature);

/**
 * Creates a new boolean object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createBooleanObject(void* state, bool value);

/**
 * Creates a new float object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createFloatObject(void* state, float value);

/**
 * Creates a new double object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createDoubleObject(void* state, double value);

/**
 * Creates a new int object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createIntegerObject(void* state, int32_t value);

/**
 * Creates a new long object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createLongObject(void* state, int64_t value);

/**
 * Creates a new short object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createShortObject(void* state, int16_t value);

/**
 * Creates a new java byte object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createByteObject(void* state, int8_t value);

/**
 * Creates a new string object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createStringObject(void* state, TextValue* value);

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
T getObjectTypeValue(void* state, void* object, std::string className, std::string getterName, std::string getterSignature);

/**
 * Get boolean value of a bool object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return bool value of the field
 */
bool getBooleanObjectValue(void* state, void* object);

/**
 * Get float value of a flaot object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return float value of the field
 */
float getFloatObjectValue(void* state, void* object);
/**
 * Get double value of a double object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return double value of the field
 */
double getDoubleObjectValue(void* state, void* object);
/**
 * Get int value of a integer object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return int value of the field
 */
int32_t getIntegerObjectValue(void* state, void* object);

/**
 * Get long value of a long object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return long value of the field
 */
int64_t getLongObjectValue(void* state, void* object);

/**
 * Get short value of a short object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return short value of the field
 */
int16_t getShortObjectValue(void* state, void* object);
/**
 * Get byte value of
 * @param state operator handler state
 * @param object object to get the field from
 * @return byte value of the field
 */
int8_t getByteObjectValue(void* state, void* object);

/**
 * Get string value of a string object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return TextValue value of the field
 */
TextValue* getStringObjectValue(void* state, void* object);

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
T getField(void* state, void* classPtr, void* objectPtr, int fieldIndex, std::string signature);

/**
 * Get the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return bool value of the field
 */
bool getBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return float value of the field
 */
float getFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return double value of the field
 */
double getDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int32_t value of the field
 */
int32_t getIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int64_t value of the field
 */
int64_t getLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int16_t value of the field
 */
int16_t getShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return int8_t value of the field
 */
int8_t getByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

/**
 * Get the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to get the field from
 * @param fieldIndex index of the field
 * @return TextValue* value of the field
 */
TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

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
void setField(void* state, void* classPtr, void* objectPtr, int fieldIndex, T value, std::string signature);

/**
 * Set the value of a boolean field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setBooleanField(void* state, void* classPtr, void* objectPtr, int fieldIndex, bool value);

/**
 * Set the value of a float field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setFloatField(void* state, void* classPtr, void* objectPtr, int fieldIndex, float value);

/**
 * Set the value of a double field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setDoubleField(void* state, void* classPtr, void* objectPtr, int fieldIndex, double value);

/**
 * Set the value of a int field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setIntegerField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int32_t value);

/**
 * Set the value of a long field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setLongField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int64_t value);

/**
 * Set the value of a short field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setShortField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int16_t value);

/**
 * Set the value of a byte field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setByteField(void* state, void* classPtr, void* objectPtr, int fieldIndex, int8_t value);

/**
 * Set the value of a string field of an object.
 * @param state operator handler state
 * @param classPtr class pointer of the object
 * @param objectPtr object to set the field to
 * @param fieldIndex index of the field
 * @param value value to set the field to
 */
void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const TextValue* value);

};// namespace NES::Runtime::Execution::Operators

#endif//ENABLE_JNI
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFUTILS_HPP_
