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
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr GeneratableGlobalThreadLocalPreAggregationOperator::create(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return std::make_shared<GeneratableGlobalThreadLocalPreAggregationOperator>(
        GeneratableGlobalThreadLocalPreAggregationOperator(id,
                                                           std::move(inputSchema),
                                                           std::move(outputSchema),
                                                           std::move(operatorHandler),
                                                           std::move(windowAggregation)));
}

GeneratableOperatorPtr GeneratableGlobalThreadLocalPreAggregationOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  std::move(windowAggregation));
}

GeneratableGlobalThreadLocalPreAggregationOperator::GeneratableGlobalThreadLocalPreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      windowAggregation(std::move(windowAggregation)), windowHandler(operatorHandler) {}

void GeneratableGlobalThreadLocalPreAggregationOperator::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowDefinition = windowHandler->getWindowDefinition();
    auto windowOperatorIndex = context->registerOperatorHandler(windowHandler);
    codegen->generateGlobalThreadLocalPreAggregationSetup(windowDefinition,
                                                          outputSchema,
                                                          context,
                                                          id,
                                                          windowOperatorIndex,
                                                          windowAggregation);
}

void GeneratableGlobalThreadLocalPreAggregationOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto handler = context->getHandlerIndex(windowHandler);
    auto windowDefinition = windowHandler->getWindowDefinition();
    codegen->generateCodeForGlobalThreadLocalPreAggregationOperator(windowDefinition, windowAggregation, context, handler);
    windowHandler = nullptr;
}

std::string GeneratableGlobalThreadLocalPreAggregationOperator::toString() const {
    return "GeneratableSlicePreAggregationOperator";
}

OperatorNodePtr GeneratableGlobalThreadLocalPreAggregationOperator::copy() {
    return create(id, inputSchema, outputSchema, windowHandler, windowAggregation);
}

}// namespace NES::QueryCompilation::GeneratableOperators