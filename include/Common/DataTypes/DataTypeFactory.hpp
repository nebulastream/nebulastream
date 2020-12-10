/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#define NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
#include <memory>
#include <vector>
namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class ValueType;
typedef std::shared_ptr<ValueType> ValueTypePtr;

enum BasicType { INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, FLOAT32, UINT64, FLOAT64, BOOLEAN, CHAR };

/**
 * @brief The data type factory offers multiple methods to construct data types
 */
class DataTypeFactory {

  public:
    /**
     * @brief Create a new Undefined type
     * @return DataTypePtr
     */
    static DataTypePtr createUndefined();

    /**
    * @brief Create a new Boolean type
    * @return DataTypePtr
    */
    static DataTypePtr createBoolean();

    /**
     * @brief Create a new Float data type, with a bits size and a lower and upper bound.
     * @param bits number of bits for this float
     * @param lowerBound lower bound for this float
     * @param upperBound upper bound for this float
     * @return DataTypePtr
     */
    static DataTypePtr createFloat(int8_t bits, double lowerBound, double upperBound);

    /**
    * @brief Create a new Float data type, which infers the bit size from the lower and upper bound.
    * @param lowerBound lower bound for this float
    * @param upperBound upper bound for this float
    * @return DataTypePtr
    */
    static DataTypePtr createFloat(double lowerBound, double upperBound);

    /**
     * @brief Creates a 32bit Float data type, which corresponds to a C++ float type.
     * @return DataTypePtr
     */
    static DataTypePtr createFloat();

    /**
    * @brief Creates a 64bit Float data type, which corresponds to a C++ double type.
    * @return DataTypePtr
    */
    static DataTypePtr createDouble();

    /**
    * @brief Create a new Integer data type, with a bits size and a lower and upper bound.
    * @param bits number of bits for this integer
    * @param lowerBound lower bound for this integer
    * @param upperBound upper bound for this integer
    * @return DataTypePtr
    */
    static DataTypePtr createInteger(int8_t bits, int64_t lowerBound, int64_t upperBound);

    /**
    * @brief Create a new Integer data type, which infers the bit size from the lower and upper bound.
    * @param lowerBound lower bound for this integer
    * @param upperBound upper bound for this integer
    * @return DataTypePtr
    */
    static DataTypePtr createInteger(int64_t lowerBound, int64_t upperBound);

    /**
    * @brief Creates a 16bit Integer data type, which corresponds to a C++ int16_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createInt8();

    /**
    * @brief Creates a 16bit Integer data type, which corresponds to a C++ int16_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createUInt8();

    /**
     * @brief Creates a 16bit Integer data type, which corresponds to a C++ int16_t type.
     * @return DataTypePtr
     */
    static DataTypePtr createInt16();

    /**
    * @brief Creates a unsighted 16bit Integer data type, which corresponds to a C++ uint16_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createUInt16();

    /**
    * @brief Creates a 32bit Integer data type, which corresponds to a C++ int32_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createInt32();

    /**
    * @brief Creates a unsighted 32bit Integer data type, which corresponds to a C++ uint32_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createUInt32();

    /**
    * @brief Creates a 364bit Integer data type, which corresponds to a C++ int64_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createInt64();

    /**
    * @brief Creates a unsighted 64bit Integer data type, which corresponds to a C++ uint64_t type.
    * @return DataTypePtr
    */
    static DataTypePtr createUInt64();

    /**
     * @brief Creates a new Array data type.
     * @param length length of the array
     * @param component component type of the array
     * @return DataTypePtr
     */
    static DataTypePtr createArray(uint64_t length, DataTypePtr component);

    /**
    * @brief Creates a new Char data type.
    * @param length length of the char
    * @return DataTypePtr
    */
    static DataTypePtr createFixedChar(uint64_t length);

    /**
    * @brief Creates a new Char data type.
    * @param length length of the char
    * @return DataTypePtr
    */
    static DataTypePtr createChar();

    /**
     * @brief Create a basic typed value. For instance a Integer with value "42".
     * @param type the data type of this value
     * @param value the value as a string
     * @return ValueTypePtr
     */
    static ValueTypePtr createBasicValue(DataTypePtr type, std::string value);

    /**
     * @brief Create a basic typed value. For instance a Integer with value "42".
     * @param type the data type as a BasicType
     * @param value the value as a string
     * @return ValueTypePtr
     */
    static ValueTypePtr createBasicValue(BasicType type, std::string value);

    /**
     * @brief Create a basic typed value. For instance a Integer with value "42".
     * @param value the value as a uint64_t
     * @return ValueTypePtr
     */
    static ValueTypePtr createBasicValue(uint64_t value);

    /**
     * @brief Create a basic typed value. For instance a Integer with value "42".
     * @param value the value as a int64_t
     * @return ValueTypePtr
     */
    static ValueTypePtr createBasicValue(int64_t value);

    /**
     * @brief Create a array typed value. For instance a Array of Integers with values ["42", "9"].
     * @param type the data type as a DataTypePtr
     * @param values the value as a vector of strings, which represent the individual values.
     * @return ValueTypePtr
     */
    static ValueTypePtr createArrayValue(DataTypePtr type, std::vector<std::string> values);

    /**
    * @brief Create a fixed char typed value. For instance ['a', 'b'].
    * @param values the value as a vector of strings, which represent the individual values.
    * @return ValueTypePtr
    */
    static ValueTypePtr createFixedCharValue(std::vector<std::string> values);

    /**
     * @brief Create a fixed char typed value. For instance ['a', 'b'].
     * @param values represents the fixed char as a single string.
     * @return ValueTypePtr
     */
    static ValueTypePtr createFixedCharValue(const char* values);

    /**
     * @brief Create a data type from a BasicType, this many is used to support the old type system API.
     * @deprecated This function may be removed in the future.
     * @param type
     * @return DataTypePtr
     */
    static DataTypePtr createType(BasicType type);
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_DATATYPEFACTORY_HPP_
