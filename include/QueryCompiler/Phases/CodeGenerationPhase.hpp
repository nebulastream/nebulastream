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
#ifndef NES_INCLUDE_QUERY_COMPILER_PHASES_CODE_GENERATION_PHASE_HPP_
#define NES_INCLUDE_QUERY_COMPILER_PHASES_CODE_GENERATION_PHASE_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <functional>

namespace NES {
namespace QueryCompilation {

/**
 * @brief Code generation phase, which generates executable machine code for pipelines, that consist of generatable operators.
 */
class CodeGenerationPhase {
  public:
    CodeGenerationPhase(CodeGeneratorPtr codeGenerator,
                        Compiler::JITCompilerPtr jitCompiler,
                        QueryCompilerOptions::CompilationStrategy compilationStrategy);

    /**
     * @brief Creates the code generation phase.
     * @return CodeGenerationPhasePtr
     */
    static CodeGenerationPhasePtr create(CodeGeneratorPtr codeGenerator,
                                         Compiler::JITCompilerPtr jitCompiler,
                                         QueryCompilerOptions::CompilationStrategy compilationStrategy);

    /**
     * @brief Generates code for all pipelines in a pipelined query plan.
     * @param pipeline PipelineQueryPlanPtr
     * @return PipelineQueryPlanPtr
     */
    PipelineQueryPlanPtr apply(PipelineQueryPlanPtr queryPlan);

    /**
     * @brief Generates code for a particular pipeline.
     * @param pipeline OperatorPipelinePtr
     * @return OperatorPipelinePtr
     */
    OperatorPipelinePtr apply(OperatorPipelinePtr pipeline);

  private:
    CodeGeneratorPtr codeGenerator;
    Compiler::JITCompilerPtr jitCompiler;
    QueryCompilerOptions::CompilationStrategy compilationStrategy;
    static void generate(const OperatorNodePtr& rootOperator,
                         const std::function<void(GeneratableOperators::GeneratableOperatorPtr)>& applyFunction);
};
}// namespace QueryCompilation
};// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_PHASES_CODE_GENERATION_PHASE_HPP_
