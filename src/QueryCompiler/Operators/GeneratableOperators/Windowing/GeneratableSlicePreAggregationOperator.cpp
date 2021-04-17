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

#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSlicePreAggregationOperator.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {
GeneratableOperatorPtr GeneratableSlicePreAggregationOperator::create(OperatorId id, SchemaPtr inputSchema,
                                                                      SchemaPtr outputSchema,
                                                                      Windowing::WindowOperatorHandlerPtr operatorHandler) {
    return std::make_shared<GeneratableSlicePreAggregationOperator>(
        GeneratableSlicePreAggregationOperator(id, inputSchema, outputSchema, operatorHandler));
}

GeneratableOperatorPtr GeneratableSlicePreAggregationOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                                                      Windowing::WindowOperatorHandlerPtr operatorHandler) {
    return create(UtilityFunctions::getNextOperatorId(), inputSchema, outputSchema, operatorHandler);
}

GeneratableSlicePreAggregationOperator::GeneratableSlicePreAggregationOperator(
    OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler)
    : OperatorNode(id), GeneratableWindowOperator(id, inputSchema, outputSchema, operatorHandler) {}

void GeneratableSlicePreAggregationOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    //auto operatorIndex = codegen->generateWindowSetup(windowDefinition, outputSchema, context, this->id);
}

void GeneratableSlicePreAggregationOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    //  codegen->generateCodeForSlicingWindow(windowDefinition, generatableWindowAggregation, context, operatorIndex);
}

const std::string GeneratableSlicePreAggregationOperator::toString() const { return "GeneratableSlicePreAggregationOperator"; }

OperatorNodePtr GeneratableSlicePreAggregationOperator::copy() { return create(id, inputSchema, outputSchema, operatorHandler); }

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES