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
#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_NAUTILUSCOMPILATIONPASE_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_NAUTILUSCOMPILATIONPASE_HPP_
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <functional>

namespace NES::QueryCompilation {

/**
 * @brief Compilation phase, which generates executable machine code for pipelines of nautilus operators.
 */
class NautilusCompilationPhase {
  public:
    /**
     * @brief Constructor to create a new NautilusCompilationPhase with a set of compilerOptions
     * @param compilerOptions
     */
    explicit NautilusCompilationPhase(const QueryCompilation::QueryCompilerOptionsPtr& compilerOptions);

    /**
     * @brief Creates the compilation phase for nautilus pipelines.
     * @return CompilationStrategy
     */
    static std::shared_ptr<NautilusCompilationPhase> create(const QueryCompilation::QueryCompilerOptionsPtr& compilerOptions);

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
    const QueryCompilation::QueryCompilerOptionsPtr compilerOptions;
};
};// namespace NES::QueryCompilation

#endif  // NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_NAUTILUSCOMPILATIONPASE_HPP_
