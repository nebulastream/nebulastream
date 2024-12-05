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
#include <string>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class AttributeField;
using AttributeFieldPtr = std::shared_ptr<AttributeField>;

/**
 * @brief Represents a typed field in a schema.
 */
class AttributeField
{
public:
    AttributeField() = default;

    /**
     * @brief Factory method to create a new field
     * @param name name of the field
     * @param dataType data type
     * @return AttributeFieldPtr
     */
    static AttributeFieldPtr create(const std::string& name, const DataTypePtr& dataType);

    [[nodiscard]] DataTypePtr getDataType() const;

    [[nodiscard]] const std::string& getName() const;

    void setName(std::string newName);

    [[nodiscard]] std::string toString() const;
    bool isEqual(const AttributeFieldPtr& attr) const;

    uint64_t hash() const;
    /**
     * @brief Make copy of this attribute
     * @return shared pointer
     */
    AttributeFieldPtr deepCopy() const;

private:
    AttributeField(std::string name, DataTypePtr dataType);

    std::string name;
    DataTypePtr dataType;
};

}
