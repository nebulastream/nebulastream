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
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/EventTimeWindow/GeneratableKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr
GeneratableKeyedSlidingWindowSink::create(OperatorId id,
                                          SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
                                          std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return std::make_shared<GeneratableKeyedSlidingWindowSink>(GeneratableKeyedSlidingWindowSink(id,
                                                                                                 std::move(inputSchema),
                                                                                                 std::move(outputSchema),
                                                                                                 std::move(operatorHandler),
                                                                                                 std::move(windowAggregation)));
}

GeneratableOperatorPtr
GeneratableKeyedSlidingWindowSink::create(SchemaPtr inputSchema,
                                          SchemaPtr outputSchema,
                                          Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
                                          std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  std::move(operatorHandler),
                  std::move(windowAggregation));
}

GeneratableKeyedSlidingWindowSink::GeneratableKeyedSlidingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      windowAggregation(std::move(windowAggregation)), windowHandler(operatorHandler) {}

void GeneratableKeyedSlidingWindowSink::generateOpen(CodeGeneratorPtr, PipelineContextPtr) {
    //   auto windowDefinition = windowHandler->getWindowDefinition();
    //codegen->generateWindowSetup(windowDefinition, outputSchema, context, id, windowHandler);
}

void GeneratableKeyedSlidingWindowSink::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto handler = context->registerOperatorHandler(windowHandler);
    auto windowDefinition = windowHandler->getWindowDefinition();
    codegen->generateCodeForKeyedSlidingWindowSink(windowDefinition, windowAggregation, context, handler, outputSchema);
    windowHandler = nullptr;
}

std::string GeneratableKeyedSlidingWindowSink::toString() const { return "GeneratableKeyedSlidingWindowSink"; }

OperatorNodePtr GeneratableKeyedSlidingWindowSink::copy() {
    return create(id, inputSchema, outputSchema, windowHandler, windowAggregation);
}

}// namespace NES::QueryCompilation::GeneratableOperators