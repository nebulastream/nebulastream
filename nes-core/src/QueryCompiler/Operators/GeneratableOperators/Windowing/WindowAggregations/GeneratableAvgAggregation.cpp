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
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableAvgAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableAvgAggregation::GeneratableAvgAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor)
    : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr
GeneratableAvgAggregation::create(const Windowing::WindowAggregationDescriptorPtr& aggregationDescriptor) {
    return std::make_shared<GeneratableAvgAggregation>(aggregationDescriptor);
}

void GeneratableAvgAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                   BinaryOperatorStatement partialRef,
                                                   RecordHandlerPtr recordHandler) {

    auto fieldReference =
        recordHandler->getAttribute(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());

    auto addSumFunctionCall = FunctionCallStatement("addToSum");
    addSumFunctionCall.addParameter(*fieldReference);
    auto updateSumStatement = partialRef.accessRef(addSumFunctionCall);

    auto setCountFunctionCall = FunctionCallStatement("addToCount");
    auto updateCountStatement = partialRef.accessRef(setCountFunctionCall);

    currentCode->addStatement(updateSumStatement.copy());
    currentCode->addStatement(updateCountStatement.copy());
}
void GeneratableAvgAggregation::generateHeaders(PipelineContextPtr context) {
    context->addHeaders({AVG_AGGR});
}
}// namespace NES::QueryCompilation::GeneratableOperators