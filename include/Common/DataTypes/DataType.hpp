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

#ifndef NES_INCLUDE_COMMON_DATA_TYPES_DATA_TYPE_HPP_
#define NES_INCLUDE_COMMON_DATA_TYPES_DATA_TYPE_HPP_

#include <memory>
#include <string>

namespace NES {

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

/**
 * @brief Base data type, which is the parent class for all other data types.
 */
class DataType {
  public:
    virtual ~DataType() = default;
    /**
     * @brief Checks if this data type is Undefined.
     */
    [[nodiscard]] virtual bool isUndefined() const;

    /**
   * @brief Checks if this data type is Boolean.
   */
    [[nodiscard]] virtual bool isBoolean() const;

    /**
    * @brief Checks if this data type is Numeric.
    */
    [[nodiscard]] virtual bool isNumeric() const;

    /**
    * @brief Checks if this data type is Integer.
    */
    [[nodiscard]] virtual bool isInteger() const;

    /**
    * @brief Checks if this data type is Float.
    */
    [[nodiscard]] virtual bool isFloat() const;

    /**
    * @brief Checks if this data type is ArrayType.
    */
    [[nodiscard]] virtual bool isArray() const;

    /**
    * @brief Checks if this data type is ArrayType.
    */
    [[nodiscard]] virtual bool isCharArray() const;

    /**
    * @brief Checks if this data type is Char.
    */
    [[nodiscard]] virtual bool isChar() const;

    template<class DataType>
    static std::shared_ptr<DataType> as(DataTypePtr ptr) {
        return std::dynamic_pointer_cast<DataType>(ptr);
    }

    /**
     * @brief Checks if two data types are equal.
     * @param otherDataType
     * @return
     */
    virtual bool isEquals(DataTypePtr otherDataType) = 0;

    /**
     * @brief Calculates the joined data type between this data type and the other.
     * If they have no possible joined data type, the coined type is Undefined.
     * @param other data type
     * @return DataTypePtr joined data type
     */
    virtual DataTypePtr join(DataTypePtr otherDataType) = 0;

    /**
     * @brief Returns a string representation of the data type.
     * @return string
     */
    virtual std::string toString() = 0;
};

}// namespace NES

#endif// NES_INCLUDE_COMMON_DATA_TYPES_DATA_TYPE_HPP_
