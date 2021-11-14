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
#include <Common/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableArrayValueType.hpp>
#include <algorithm>
#include <sstream>

namespace NES::QueryCompilation {
CodeExpressionPtr GeneratableArrayValueType::getCodeExpression() const noexcept {
    bool const containsChars = valueType->dataType->isCharArray();

    std::stringstream str;
    str << "NES::ExecutableTypes::Array {";
    if (containsChars) {

        bool nullTerminated = false;
        for (std::size_t i = 0; i < values.size(); ++i) {
            if (i) {
                str << ", ";
            }

            auto const v = !values[i].empty() ? values[i][0] : 0;
            if (std::isprint(v)) {
                str << '\'' << values[i] << '\'';
            } else {
                nullTerminated |= !v;
                str << "static_cast<char>(" << static_cast<int>(v) << ')';
            }
        }

        // if no null terminator is given, explicitly add it.
        if (!nullTerminated) {
            if (!values.empty()) {
                str << ", \\0";
            } else {
                str << "\\0";
            }
        }
    } else {

        for (std::size_t i = 0; i < values.size(); ++i) {
            if (i != 0) {
                str << ", ";
            }
            str << values.at(i);
        }
    }

    str << '}';

    return std::make_shared<CodeExpression>(str.str());
}
}// namespace NES::QueryCompilation