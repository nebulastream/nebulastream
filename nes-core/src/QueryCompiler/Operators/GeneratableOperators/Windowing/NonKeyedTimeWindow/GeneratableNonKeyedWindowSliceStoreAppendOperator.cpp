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
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/NonKeyedTimeWindow/GeneratableNonKeyedWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/Core.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr GeneratableNonKeyedWindowSliceStoreAppendOperator::create(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return std::make_shared<GeneratableNonKeyedWindowSliceStoreAppendOperator>(
        GeneratableNonKeyedWindowSliceStoreAppendOperator(id,
                                                          std::move(inputSchema),
                                                          std::move(outputSchema),
                                                          std::move(operatorHandler),
                                                          std::move(windowAggregation)));
}

GeneratableOperatorPtr GeneratableNonKeyedWindowSliceStoreAppendOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return create(getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  std::move(windowAggregation));
}

GeneratableNonKeyedWindowSliceStoreAppendOperator::GeneratableNonKeyedWindowSliceStoreAppendOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::NonKeyedGlobalSliceStoreAppendOperatorHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      windowAggregation(std::move(windowAggregation)), windowHandler(operatorHandler) {}

void GeneratableNonKeyedWindowSliceStoreAppendOperator::generateOpen(CodeGeneratorPtr, PipelineContextPtr) {}

void GeneratableNonKeyedWindowSliceStoreAppendOperator::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto handler = context->registerOperatorHandler(windowHandler);
    auto windowDefinition = windowHandler->getWindowDefinition();
    codegen->generateCodeForNonKeyedSliceStoreAppend(context, handler);
    windowHandler = nullptr;
}

std::string GeneratableNonKeyedWindowSliceStoreAppendOperator::toString() const {
    return "GeneratableNonKeyedWindowSliceStoreAppendOperator";
}

OperatorNodePtr GeneratableNonKeyedWindowSliceStoreAppendOperator::copy() {
    return create(id, inputSchema, outputSchema, windowHandler, windowAggregation);
}

}// namespace NES::QueryCompilation::GeneratableOperators