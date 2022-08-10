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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/TensorType.hpp>
#include <Common/ValueTypes/BasicValue.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTensorValueType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <algorithm>
#include <sstream>

namespace NES::QueryCompilation {

CodeExpressionPtr GeneratableTensorValueType::getCodeExpression() const noexcept {

    if (valueType->dataType->isTensor()) {
        auto tensor = valueType->dataType->as<TensorType>(valueType->dataType);
        auto tf = CodeGenerator::getTypeFactory();
        auto comp = tf->createDataType(tensor->component);

        std::stringstream str;
        str << "NES::ExecutableTypes::Tensor<";
        str << comp->getCode()->code_ << ", ";

        for (std::size_t i = 0; i < tensor->shape.size(); ++i) {
            if (i != 0) {
                str << ", ";
            }
            str << tensor->shape.at(i);
        }

        str << "> {";

        for (std::size_t i = 0; i < values.size(); ++i) {
            if (i != 0) {
                str << ", ";
            }
            str << values.at(i);
        }

        str << '}';

        return std::make_shared<CodeExpression>(str.str());
    } else {
      return nullptr;
    }
}
}// namespace NES::QueryCompilation