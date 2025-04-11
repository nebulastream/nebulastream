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
#include <Common/DataTypes/DataType.hpp>

namespace NES
{


/**
 * @brief Represents a typed field in a schema.
 */
class AttributeField
{
public:
    AttributeField() = default;
    AttributeField(std::string name, std::shared_ptr<DataType> dataType);

    [[nodiscard]] std::shared_ptr<DataType> getDataType() const;

    [[nodiscard]] std::string getName() const;

    void setName(std::string newName);

    [[nodiscard]] std::string toString() const;
    [[nodiscard]] bool isEqual(const AttributeField& attr) const;

    uint64_t hash() const;

private:
    std::string name;
    std::shared_ptr<DataType> dataType;
};

}
