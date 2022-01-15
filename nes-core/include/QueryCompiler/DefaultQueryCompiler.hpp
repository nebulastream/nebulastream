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
#ifndef NES_INCLUDE_QUERY_COMPILER_DEFAULT_QUERY_COMPILER_HPP_
#define NES_INCLUDE_QUERY_COMPILER_DEFAULT_QUERY_COMPILER_HPP_
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES::QueryCompilation {

/**
 * @brief The default query compiler for NES.
 * Subclasses can introduce new compilation steps.
 */
class DefaultQueryCompiler : public QueryCompiler {
  public:
    /**
     * @brief Creates a new instance of the DefaultQueryCompiler, with a set of options and phases.
     * @param options QueryCompilationOptions.
     * @param phaseFactory Factory which allows the injection of query optimization phases.
     * @return QueryCompilerPtr
     */
    static QueryCompilerPtr create(QueryCompilerOptionsPtr const& options,
                                   Phases::PhaseFactoryPtr const& phaseFactory,
                                   Compiler::JITCompilerPtr jitCompiler);

    /**
    * @brief Submits a new query compilation request for compilation.
    * @param request The compilation request.
    * @return QueryCompilationResultPtr result for the query compilation.
    */
    QueryCompilationResultPtr compileQuery(QueryCompilationRequestPtr request) override;

    std::map<std::string, std::map<std::string, std::string>> getDumpContextInfo() override;

  protected:
    DefaultQueryCompiler(QueryCompilerOptionsPtr const& options,
                         Phases::PhaseFactoryPtr const& phaseFactory,
                         Compiler::JITCompilerPtr jitCompiler);
    LowerLogicalToPhysicalOperatorsPtr lowerLogicalToPhysicalOperatorsPhase;
    LowerPhysicalToGeneratableOperatorsPtr lowerPhysicalToGeneratableOperatorsPhase;
    LowerToExecutableQueryPlanPhasePtr lowerToExecutableQueryPlanPhase;
    PipeliningPhasePtr pipeliningPhase;
    AddScanAndEmitPhasePtr addScanAndEmitPhase;
    BufferOptimizationPhasePtr bufferOptimizationPhase;
    PredicationOptimizationPhasePtr predicationOptimizationPhase;
    CodeGenerationPhasePtr codeGenerationPhase;

};

}// namespace NES::QueryCompilation
#endif// NES_INCLUDE_QUERY_COMPILER_DEFAULT_QUERY_COMPILER_HPP_
