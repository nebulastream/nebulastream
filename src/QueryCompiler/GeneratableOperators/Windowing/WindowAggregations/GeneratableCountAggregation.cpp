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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES {

GeneratableCountAggregation::GeneratableCountAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr GeneratableCountAggregation::create(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) {
    return std::make_shared<GeneratableCountAggregation>(aggregationDescriptor);
}

void GeneratableCountAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                     BinaryOperatorStatement partialRef,
                                                     StructDeclaration inputStruct,
                                                     BinaryOperatorStatement) {
    auto varDeclInput = inputStruct.getVariableDeclaration(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto increment = ++partialRef;
    auto updatedPartial = partialRef.assign(increment);
    currentCode->addStatement(std::make_shared<BinaryOperatorStatement>(updatedPartial));
}

}// namespace NES