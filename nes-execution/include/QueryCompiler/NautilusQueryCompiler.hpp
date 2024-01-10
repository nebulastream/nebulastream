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
#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
#include <QueryCompiler/QueryCompiler.hpp>

namespace NES::QueryCompilation {

class LowerPhysicalToNautilusOperators;
using LowerPhysicalToNautilusOperatorsPtr = std::shared_ptr<LowerPhysicalToNautilusOperators>;
class NautilusCompilationPhase;
using NautilusCompilationPhasePtr = std::shared_ptr<NautilusCompilationPhase>;

/**
 * @brief A QueryCompiler which uses the nautilus operators for code generations.
 */
class NautilusQueryCompiler : public QueryCompilation::QueryCompiler {
  public:
    QueryCompilation::QueryCompilationResultPtr compileQuery(QueryCompilation::QueryCompilationRequestPtr request) override;
    /**
     * @brief Creates a new instance of the NautilusQueryCompiler, with a set of options and phases.
     * @param options QueryCompilationOptions.
     * @param phaseFactory Factory which allows the injection of query optimization phases.
     * @param sourceSharing indicates if source sharing should be used
     * @return QueryCompilerPtr
     */
    static QueryCompilerPtr
    create(QueryCompilerOptionsPtr const& options, Phases::PhaseFactoryPtr const& phaseFactory, bool sourceSharing = false);

  protected:
    NautilusQueryCompiler(QueryCompilation::QueryCompilerOptionsPtr const& options,
                          Phases::PhaseFactoryPtr const& phaseFactory,
                          bool sourceSharing);
    QueryCompilation::LowerLogicalToPhysicalOperatorsPtr lowerLogicalToPhysicalOperatorsPhase;
    QueryCompilation::LowerPhysicalToNautilusOperatorsPtr lowerPhysicalToNautilusOperatorsPhase;
    QueryCompilation::NautilusCompilationPhasePtr compileNautilusPlanPhase;
    QueryCompilation::LowerToExecutableQueryPlanPhasePtr lowerToExecutableQueryPlanPhase;
    QueryCompilation::PipeliningPhasePtr pipeliningPhase;
    QueryCompilation::AddScanAndEmitPhasePtr addScanAndEmitPhase;
    bool sourceSharing;
};

}// namespace NES::QueryCompilation

#endif // NES_EXECUTION_INCLUDE_QUERYCOMPILER_NAUTILUSQUERYCOMPILER_HPP_
