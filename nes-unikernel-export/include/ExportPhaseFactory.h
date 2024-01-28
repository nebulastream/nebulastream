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

#ifndef NES_EXPORTPHASEFACTORY_H
#define NES_EXPORTPHASEFACTORY_H

#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES::QueryCompilation::Phases {
class ExportPhaseFactory : public PhaseFactory {
  public:
    virtual ~ExportPhaseFactory() = default;
    static PhaseFactoryPtr create();
    LowerLogicalToPhysicalOperatorsPtr createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) override;
    PipeliningPhasePtr createPipeliningPhase(QueryCompilerOptionsPtr options) override;
    AddScanAndEmitPhasePtr createAddScanAndEmitPhase(QueryCompilerOptionsPtr options) override;
    LowerPhysicalToGeneratableOperatorsPtr createLowerPhysicalToGeneratableOperatorsPhase(QueryCompilerOptionsPtr options) {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
    LowerToExecutableQueryPlanPhasePtr createLowerToExecutableQueryPlanPhase(QueryCompilerOptionsPtr options,
                                                                             bool sourceSharing) {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
    BufferOptimizationPhasePtr createBufferOptimizationPhase(QueryCompilerOptionsPtr options) override;
    PredicationOptimizationPhasePtr createPredicationOptimizationPhase(QueryCompilerOptionsPtr options) {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
};
}// namespace NES::QueryCompilation::Phases
#endif//NES_EXPORTPHASEFACTORY_H
