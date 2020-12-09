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
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCombiningWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

namespace NES {

void GeneratableCombiningWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableCombiningWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowHandler = createWindowHandler(outputSchema);
    context->setWindow(windowHandler);
    codegen->generateCodeForCombiningWindow(getWindowDefinition(), generatableWindowAggregation, context);
}

GeneratableDistributedlWindowCombinerOperatorPtr
GeneratableCombiningWindowOperator::create(Windowing::LogicalWindowDefinitionPtr windowDefinition,
                                           GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id) {
    return std::make_shared<GeneratableCombiningWindowOperator>(
        GeneratableCombiningWindowOperator(std::move(windowDefinition), std::move(generatableWindowAggregation), id));
}

GeneratableCombiningWindowOperator::GeneratableCombiningWindowOperator(
    Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation,
    OperatorId id)
    : GeneratableWindowOperator(std::move(windowDefinition), std::move(generatableWindowAggregation), id) {}

const std::string GeneratableCombiningWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCombiningWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES