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

#ifndef INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
#define INCLUDE_CORE_ATTRIBUTEFIELD_HPP_

#include <memory>
#include <string>

namespace NES {

class DataType;
typedef std::shared_ptr<DataType> DataTypePtr;

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

/**
 * @brief Represents a typed field in a schema.
 */
class AttributeField {
  public:
    AttributeField() = default;
    AttributeField(const std::string& name, DataTypePtr dataType);

    /**
     * @brief Factory method to create a new field
     * @param name name of the field
     * @param dataType data type
     * @return AttributeFieldPtr
     */
    static AttributeFieldPtr create(std::string name, DataTypePtr dataType);

    DataTypePtr getDataType() const;
    const std::string toString() const;
    bool isEqual(AttributeFieldPtr attr);

    std::string name;
    DataTypePtr dataType;
};

}// namespace NES
#endif//INCLUDE_CORE_ATTRIBUTEFIELD_HPP_
