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
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/NonKeyedTimeWindow/GeneratableNonKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Core.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr GeneratableNonKeyedSlidingWindowSink::create(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return std::make_shared<GeneratableNonKeyedSlidingWindowSink>(
        GeneratableNonKeyedSlidingWindowSink(id,
                                             std::move(inputSchema),
                                             std::move(outputSchema),
                                             std::move(operatorHandler),
                                             std::move(windowAggregation)));
}

GeneratableOperatorPtr GeneratableNonKeyedSlidingWindowSink::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return create(getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  std::move(windowAggregation));
}

GeneratableNonKeyedSlidingWindowSink::GeneratableNonKeyedSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedSlidingWindowSinkOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      windowAggregation(std::move(windowAggregation)), windowHandler(operatorHandler) {}

void GeneratableNonKeyedSlidingWindowSink::generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowOperatorIndex = context->registerOperatorHandler(windowHandler);
    codegen->generateNonKeyedSlidingWindowOperatorSetup(windowHandler->getWindowDefinition(),
                                                        context,
                                                        id,
                                                        windowOperatorIndex,
                                                        windowAggregation);
}

void GeneratableNonKeyedSlidingWindowSink::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto handler = context->getHandlerIndex(windowHandler);
    auto windowDefinition = windowHandler->getWindowDefinition();
    codegen->generateCodeForNonKeyedSlidingWindowSink(windowDefinition, windowAggregation, context, handler, outputSchema);
    windowHandler = nullptr;
}

std::string GeneratableNonKeyedSlidingWindowSink::toString() const { return "GeneratableGlobalSlidingWindowSink"; }

OperatorNodePtr GeneratableNonKeyedSlidingWindowSink::copy() {
    return create(id, inputSchema, outputSchema, windowHandler, windowAggregation);
}

}// namespace NES::QueryCompilation::GeneratableOperators