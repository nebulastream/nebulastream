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
#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
#include <QueryCompiler/Phases/PhaseFactory.hpp>

namespace NES::QueryCompilation::Phases {

/**
 * @brief The default phase factory creates a default set of phases.
 */
class DefaultPhaseFactory : public PhaseFactory {
  public:
    virtual ~DefaultPhaseFactory() = default;
    static PhaseFactoryPtr create();
    LowerLogicalToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) override;
    PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) override;
    AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) override;
    LowerToExecutableQueryPlanPhasePtr createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options,
                                                                             bool sourceSharing) override;
    BufferOptimizationPhasePtr createBufferOptimizationPhase(QueryCompilerOptionsPtr options) override;
};

}// namespace NES::QueryCompilation::Phases

#endif// NES_RUNTIME_INCLUDE_QUERYCOMPILER_PHASES_DEFAULTPHASEFACTORY_HPP_
