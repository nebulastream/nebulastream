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

#ifndef NES_INCLUDE_API_ATTRIBUTEFIELD_HPP_
#define NES_INCLUDE_API_ATTRIBUTEFIELD_HPP_

#include <memory>
#include <string>

namespace NES {

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class AttributeField;
using AttributeFieldPtr = std::shared_ptr<AttributeField>;

/**
 * @brief Represents a typed field in a schema.
 */
class AttributeField {
  public:
    AttributeField() = default;
    AttributeField(std::string name, DataTypePtr dataType);

    /**
     * @brief Factory method to create a new field
     * @param name name of the field
     * @param dataType data type
     * @return AttributeFieldPtr
     */
    static AttributeFieldPtr create(const std::string& name, const DataTypePtr& dataType);

    [[nodiscard]] DataTypePtr getDataType() const;

    [[nodiscard]] std::string getName() const;

    void setName(std::string newName);

    [[nodiscard]] std::string toString() const;
    bool isEqual(const AttributeFieldPtr& attr);

    /**
     * @brief Make copy of this attribute
     * @return shared pointer
     */
    AttributeFieldPtr copy();

  private:
    std::string name;
    DataTypePtr dataType;
};

}// namespace NES
#endif  // NES_INCLUDE_API_ATTRIBUTEFIELD_HPP_
