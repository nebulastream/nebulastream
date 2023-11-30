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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Util/JNI/JNI.hpp>
#include <Util/SourceLocation.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * Finds the input class in the JVM and returns a jclass object pointer.
 * @param state operator handler state
 * @return jclass input class object pointer
 */
void* findInputClass(void* state);

void* getObjectClass(void* object);

void freeObject(void* object);

void* allocateObject(void* clazzPtr);

/**
 * Creates a new boolean object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createBooleanObject(bool value);

/**
 * Creates a new float object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createFloatObject(float value);

/**
 * Creates a new double object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createDoubleObject(double value);

/**
 * Creates a new int object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createIntegerObject(int32_t value);

/**
 * Creates a new long object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createLongObject(int64_t value);

/**
 * Creates a new short object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createShortObject(int16_t value);

/**
 * Creates a new java byte object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createByteObject(int8_t value);

/**
 * Creates a new string object and sets its value in the constructor.
 * @param state operator handler state
 * @param value value to set
 */
void* createStringObject(Nautilus::TextValue* value);

/**
 * Get boolean value of a bool object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return bool value of the field
 */
bool getBooleanObjectValue(void* object);

/**
 * Get float value of a flaot object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return float value of the field
 */
float getFloatObjectValue(void* object);
/**
 * Get double value of a double object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return double value of the field
 */
double getDoubleObjectValue(void* object);
/**
 * Get int value of a integer object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return int value of the field
 */
int32_t getIntegerObjectValue(void* object);

/**
 * Get long value of a long object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return long value of the field
 */
int64_t getLongObjectValue(void* object);

/**
 * Get short value of a short object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return short value of the field
 */
int16_t getShortObjectValue(void* object);
/**
 * Get byte value of
 * @param state operator handler state
 * @param object object to get the field from
 * @return byte value of the field
 */
int8_t getByteObjectValue(void* object);

/**
 * Get string value of a string object.
 * @param state operator handler state
 * @param object object to get the field from
 * @return TextValue value of the field
 */
Nautilus::TextValue* getStringObjectValue(void* object);

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
Nautilus::TextValue* getStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex);

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
void setStringField(void* state, void* classPtr, void* objectPtr, int fieldIndex, const Nautilus::TextValue* value);

};// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_JAVAUDFUTILS_HPP_
