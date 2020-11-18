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

#include <Common/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableArrayValueType.hpp>
#include <sstream>

namespace NES {

GeneratableArrayValueType::GeneratableArrayValueType(ValueTypePtr valueTypePtr, std::vector<std::string> values, bool isString)
    : GeneratableValueType(), valueType(valueTypePtr), values(values), isString(isString) {}

CodeExpressionPtr GeneratableArrayValueType::getCodeExpression() {
    std::stringstream str;
    if (isString) {
        str << "\"" << values.at(0) << "\"";
        return std::make_shared<CodeExpression>(str.str());
    }
    bool isCharArray = (valueType->isCharValue());
    str << "{";
    u_int32_t i;
    for (i = 0; i < values.size(); i++) {
        if (i != 0)
            str << ", ";
        if (isCharArray)
            str << "\'";
        str << values.at(i);
        if (isCharArray)
            str << "\'";
    }
    str << "}";
    return std::make_shared<CodeExpression>(str.str());
}
}// namespace NES