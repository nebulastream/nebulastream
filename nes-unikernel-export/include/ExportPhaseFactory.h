//
// Created by ls on 30.09.23.
//

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
    BufferOptimizationPhasePtr createBufferOptimizationPhase(QueryCompilerOptionsPtr options) {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
    PredicationOptimizationPhasePtr createPredicationOptimizationPhase(QueryCompilerOptionsPtr options) {
        NES_THROW_RUNTIME_ERROR("Not Implemented");
    }
};
}// namespace NES::QueryCompilation::Phases
#endif//NES_EXPORTPHASEFACTORY_H
