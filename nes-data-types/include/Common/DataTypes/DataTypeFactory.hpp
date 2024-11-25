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

#pragma once
#include <memory>
#include <vector>
#include <Common/DataTypes/BasicTypes.hpp>
namespace NES
{

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class VariableSizedDataType;
using VariableSizedDataTypePtr = std::shared_ptr<VariableSizedDataType>;

/**
 * @brief The data type factory offers multiple methods to construct data types
 */
class DataTypeFactory
{
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
    * @brief Creates a new VariableSizedDataType data type.
    * @return DataTypePtr
    */
    static DataTypePtr createVariableSizedData();

    /**
    * @brief Creates a new Char data type.
    * @param length length of the char
    * @return DataTypePtr
    */
    static DataTypePtr createChar();

    /**
     * @brief Create a data type from a BasicType, this many is used to support the old type system API.
     * @deprecated This function may be removed in the future.
     * @param type
     * @return DataTypePtr
     */
    static DataTypePtr createType(BasicType type);

    /**
     * @brief If the given lowerBound is tighter than the one on the given stamp, create a new stamp with tighter lowerBound. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param minLowerBound lowerBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndIncreaseLowerBound(DataTypePtr stamp, double minLowerBound);
    /**
     * @brief If the given lowerBound is tighter than the one on the given stamp, create a new stamp with tighter lowerBound. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param minLowerBound lowerBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndIncreaseLowerBound(DataTypePtr stamp, int64_t minLowerBound);
    /**
     * @brief If the given upperBound is tighter than the one on the given stamp, create a new stamp with tighter upperBound. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param maxUpperBound upperBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndDecreaseUpperBound(DataTypePtr stamp, double maxUpperBound);
    /**
     * @brief If the given upperBound is tighter than the one on the given stamp, create a new stamp with tighter upperBound. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param maxUpperBound upperBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndDecreaseUpperBound(DataTypePtr stamp, int64_t maxUpperBound);

    /// functions to increase lower and decrease upper bound at the same time:
    /**
     * @brief If the given bounds are tighter than the one on the given stamp, create a new stamp with tighter bounds. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param minLowerBound lowerBound that can not be exceeded in the returned type
     * @param maxUpperBound upperBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndTightenBounds(DataTypePtr stamp, int64_t minLowerBound, int64_t maxUpperBound);
    /**
     * @brief If the given bounds are tighter than the one on the given stamp, create a new stamp with tighter bounds. If not, return original stamp.
     * @param stamp Datatype to copy
     * @param minLowerBound lowerBound that can not be exceeded in the returned type
     * @param maxUpperBound upperBound that can not be exceeded in the returned type
     * @return DataTypePtr
     */
    static DataTypePtr copyTypeAndTightenBounds(DataTypePtr stamp, double minLowerBound, double maxUpperBound);

    /**
     * @brief Creates Integer stamp with bounds equivalent to passed FLoat stamp
     * @param Integer stamp Datatype to copy. If float is passed the same stamp is returned. Error at non-numeric stamp.
     * @return DataTypePtr
     */
    static DataTypePtr createFloatFromInteger(DataTypePtr stamp);
};

}
