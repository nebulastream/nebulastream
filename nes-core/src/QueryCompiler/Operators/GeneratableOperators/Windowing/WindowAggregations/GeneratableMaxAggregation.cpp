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
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Runtime/RuntimeHeaders.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableMaxAggregation::GeneratableMaxAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor)
    : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr
GeneratableMaxAggregation::create(const Windowing::WindowAggregationDescriptorPtr& aggregationDescriptor) {
    return std::make_shared<GeneratableMaxAggregation>(aggregationDescriptor);
}

void GeneratableMaxAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   RecordHandlerPtr recordHandler) {
    auto fieldReference =
        recordHandler->getAttribute(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());
    auto ifStatement = IF(partialRef < *fieldReference, partialRef.assign(fieldReference));
    currentCode->addStatement(ifStatement.createCopy());
}
void GeneratableMaxAggregation::generateHeaders(PipelineContextPtr context) {
    context->addHeaders({MAX_AGGR});
}
}// namespace NES::QueryCompilation::GeneratableOperators