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

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <API/AttributeField.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

AttributeField::AttributeField(std::string name, std::shared_ptr<DataType> dataType) : name(std::move(name)), dataType(std::move(dataType))
{
}

std::string AttributeField::getName() const
{
    return name;
}

void AttributeField::setName(std::string newName)
{
    this->name = std::move(newName);
}

std::shared_ptr<DataType> AttributeField::getDataType() const
{
    INVARIANT(dataType != nullptr, "DataType pointer is null in AttributeField");
    return dataType;
}

std::string AttributeField::toString() const
{
    std::stringstream ss;
    ss << name << ":" << dataType->toString();
    return ss.str();
}

bool AttributeField::isEqual(const AttributeField& attr) const
{
    const bool equalDataType = (this->dataType == nullptr and attr.dataType == nullptr) or (*this->dataType == *attr.dataType);
    return (attr.name == name) && equalDataType;
}

uint64_t AttributeField::hash() const
{
    uint64_t hashValue = 0;
    hashValue = hashValue * 0x9e3779b1 + std::hash<std::string>{}(name);
    hashValue = hashValue * 0x9e3779b1 + std::hash<std::string>{}(dataType->toString());
    return hashValue;
}

}
