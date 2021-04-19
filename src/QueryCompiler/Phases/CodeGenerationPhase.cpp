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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Phases/CodeGenerationPhase.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>

namespace NES {
namespace QueryCompilation {

CodeGenerationPhasePtr CodeGenerationPhase::create() {
    return std::make_shared<CodeGenerationPhase>();
}

NodeEngine::Execution::ExecutablePipelineStagePtr CodeGenerationPhase::apply(OperatorPipelinePtr pipeline) {
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    auto pipelineRoot = pipeline->getQueryPlan()->getRootOperators();
    NES_ASSERT(pipelineRoot.size() == 1, "A pipeline should have a single root operator.");

    generate(pipelineRoot[0], [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateOpen(codeGenerator, context);
    });

    generate(pipelineRoot[0], [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateExecute(codeGenerator, context);
    });

    generate(pipelineRoot[0], [&codeGenerator, &context](GeneratableOperators::GeneratableOperatorPtr operatorNode) {
        operatorNode->generateClose(codeGenerator, context);
    });
    return codeGenerator->compile(context);;
}

void CodeGenerationPhase::generate(OperatorNodePtr rootOperator,
                                   std::function<void(GeneratableOperators::GeneratableOperatorPtr operatorNode)> applyFunction) {
   auto iterator = DepthFirstNodeIterator(rootOperator);
   for(auto node: iterator){
       NES_ASSERT(node->instanceOf<GeneratableOperators::GeneratableOperator>(), "Operator should be of type GeneratableOperator but it is a " + node->toString());
       auto generatableOperator = node->as<GeneratableOperators::GeneratableOperator>();
       applyFunction(generatableOperator);
   }
}

}// namespace QueryCompilation
}// namespace NES
