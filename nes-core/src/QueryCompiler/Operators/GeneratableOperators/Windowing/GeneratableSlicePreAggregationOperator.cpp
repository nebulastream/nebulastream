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

#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSlicePreAggregationOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr GeneratableSlicePreAggregationOperator::create(OperatorId id,
                                                                      SchemaPtr inputSchema,
                                                                      SchemaPtr outputSchema,
                                                                      Windowing::WindowOperatorHandlerPtr operatorHandler,
                                                                      GeneratableWindowAggregationPtr windowAggregation) {
    return std::make_shared<GeneratableSlicePreAggregationOperator>(
        GeneratableSlicePreAggregationOperator(id,
                                               std::move(inputSchema),
                                               std::move(outputSchema),
                                               std::move(operatorHandler),
                                               std::move(windowAggregation)));
}

GeneratableOperatorPtr GeneratableSlicePreAggregationOperator::create(SchemaPtr inputSchema,
                                                                      SchemaPtr outputSchema,
                                                                      Windowing::WindowOperatorHandlerPtr operatorHandler,
                                                                      GeneratableWindowAggregationPtr windowAggregation) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  std::move(windowAggregation));
}

GeneratableSlicePreAggregationOperator::GeneratableSlicePreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::WindowOperatorHandlerPtr operatorHandler,
    GeneratableWindowAggregationPtr windowAggregation)
    : OperatorNode(id),
      GeneratableWindowOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(operatorHandler)),
      windowAggregation(std::move(windowAggregation)) {}

void GeneratableSlicePreAggregationOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowDefinition = operatorHandler->getWindowDefinition();
    codegen->generateWindowSetup(windowDefinition, outputSchema, context, id, operatorHandler);
}

void GeneratableSlicePreAggregationOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto handler = context->getHandlerIndex(operatorHandler);
    auto windowDefinition = operatorHandler->getWindowDefinition();
    codegen->generateCodeForSlicingWindow(windowDefinition, windowAggregation, context, handler);
}

std::string GeneratableSlicePreAggregationOperator::toString() const { return "GeneratableSlicePreAggregationOperator"; }

OperatorNodePtr GeneratableSlicePreAggregationOperator::copy() {
    return create(id, inputSchema, outputSchema, operatorHandler, windowAggregation);
}
void GeneratableSlicePreAggregationOperator::generateHeaders(PipelineContextPtr context) {
    //generate headers of aggregation type
    windowAggregation->generateHeaders(context);
    //generate headers of trigger policy
    operatorHandler->getWindowDefinition()->getTriggerPolicy()->generateHeaders(context);
    //generate headers of parent class
    GeneratableWindowOperator::generateHeaders(context);
};

}// namespace NES::QueryCompilation::GeneratableOperators