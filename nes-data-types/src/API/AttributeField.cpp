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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <sstream>
#include <utility>

namespace NES {

AttributeField::AttributeField(std::string name, DataTypePtr dataType) : name(std::move(name)), dataType(std::move(dataType)) {}

AttributeFieldPtr AttributeField::create(const std::string& name, const DataTypePtr& dataType) {
    return std::make_shared<AttributeField>(name, dataType);
}

const std::string& AttributeField::getName() const { return name; }

void AttributeField::setName(std::string newName) { this->name = std::move(newName); }

DataTypePtr AttributeField::getDataType() const { return dataType; }

std::string AttributeField::toString() const {
    std::stringstream ss;
    ss << name << ":" << dataType->toString();
    return ss.str();
}

bool AttributeField::isEqual(const AttributeFieldPtr& attr) {
    if (!attr) {
        return false;
    }

    const bool equalDataType = (this->dataType == nullptr && attr->dataType == nullptr)
        || (this->dataType != nullptr && attr->dataType != nullptr && this->dataType->isEquals(attr->dataType));
    return (attr->name == name) && equalDataType;
}

std::pair<std::string_view, std::string_view> AttributeField::splitSourcePrefix() const {
    auto it = this->name.find_first_of("$");
    if (it == std::string::npos)
        return std::make_pair("", this->name);

    auto sv = std::string_view(this->name);
    return std::make_pair(sv.substr(0, it), sv.substr(it + 1));
}

bool AttributeField::isEqualIgnoringPrefix(const AttributeFieldPtr& attr) const {
    auto [s1, n1] = attr->splitSourcePrefix();
    auto [s2, n2] = attr->splitSourcePrefix();
    return n1 == n2;
}

AttributeFieldPtr AttributeField::copy() { return create(name, dataType); }

}// namespace NES
