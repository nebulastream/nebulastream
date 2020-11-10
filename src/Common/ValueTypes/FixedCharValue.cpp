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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ValueTypes/FixedCharValue.hpp>
#include <sstream>
namespace NES {

FixedCharValue::FixedCharValue(std::vector<std::string> values) : ValueType(DataTypeFactory::createFixedChar(values.size())), values(values), isString(false) {}

FixedCharValue::FixedCharValue(const std::string& value) : ValueType(DataTypeFactory::createFixedChar(value.size())), isString(true) {
    std::stringstream str;
    values.push_back(value);
}

bool FixedCharValue::isCharValue() {
    return true;
}

bool FixedCharValue::isEquals(ValueTypePtr) {
    return false;
}

std::string FixedCharValue::toString() {
    return std::string();
}

const std::vector<std::string>& FixedCharValue::getValues() const {
    return values;
}

bool FixedCharValue::getIsString() const {
    return isString;
}

}// namespace NES