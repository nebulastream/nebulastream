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

#ifndef NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
#define NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_

#include <memory>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class PhysicalType;
typedef std::shared_ptr<PhysicalType> PhysicalTypePtr;

/**
 * @brief The physical data type represents the physical representation of a NES data type.
 */
class PhysicalType {
  public:
    inline PhysicalType(DataTypePtr type) noexcept : type(type) {}

    /**
     * @brief Returns the number of bytes occupied by this data type.
     * @return u_int64_t
     */
    virtual uint64_t size() const = 0;

    /**
     * @brief Converts the binary representation of this value to a string.
     * @param rawData a pointer to the raw value
     * @return string
     */
    virtual std::string convertRawToString(void const* rawData) const noexcept = 0;

    /**
     * @brief Returns the string representation of this physical data type.
     * @return string
     */
    virtual std::string toString() const noexcept = 0;

    /**
     * @brief Indicates if this is a basic data type.
     * @return true if type is basic type
     */
    virtual bool isBasicType() const noexcept { return false; }

    /**
     * @brief Indicates if this is a array data type.
     * @return true if type is array
     */
    virtual bool isArrayType() const noexcept { return false; };

    /// @brief true only for arrays which contain chars as their immediate child type.
    virtual bool isCharArrayType() const noexcept { return false; };

    /// Type that is contained by this PhysicalType container
    DataTypePtr const type;
};

}// namespace NES

#endif//NES_INCLUDE_DATATYPES_PHYSICALTYPES_PHYSICALTYPES_HPP_
