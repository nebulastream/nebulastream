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


#include <Common/DataTypes/DataType.hpp>
#include <Common/ValueTypes/ArrayValue.hpp>

namespace NES {

ArrayValue::ArrayValue(DataTypePtr type, std::vector<std::string> values) : ValueType(type), values(values) {
}

std::vector<std::string> ArrayValue::getValues() {
    return values;
}

bool ArrayValue::isArrayValue() {
    return true;
}

std::string ArrayValue::toString() {
    return "ArrayValue";
}

bool ArrayValue::isEquals(ValueTypePtr valueType) {
    if (!valueType->isArrayValue()) {
        return false;
    }
    auto arrayValue = std::dynamic_pointer_cast<ArrayValue>(valueType);
    return getType()->isEquals(arrayValue->getType()) && values == arrayValue->values;
}

}// namespace NES