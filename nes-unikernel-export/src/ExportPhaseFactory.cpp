//
// Created by ls on 30.09.23.
//

#include "ExportPhaseFactory.h"
#include "QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp"
#include "QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp"
#include "QueryCompiler/Phases/Pipelining/OperatorAtATimePolicy.hpp"
#include "QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp"
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/BufferOptimizationPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
namespace NES::QueryCompilation::Phases {
PhaseFactoryPtr ExportPhaseFactory::create() { return std::make_shared<ExportPhaseFactory>(); }

PipeliningPhasePtr ExportPhaseFactory::createPipeliningPhase(QueryCompilerOptionsPtr options) {
    switch (options->getPipeliningStrategy()) {
        case PipeliningStrategy::OPERATOR_FUSION: {
            NES_DEBUG("Create pipelining phase with fuse policy");
            auto operatorFusionPolicy = FuseNonPipelineBreakerPolicy::create();
            return DefaultPipeliningPhase::create(operatorFusionPolicy);
        };
        case PipeliningStrategy::OPERATOR_AT_A_TIME: {
            NES_DEBUG("Create pipelining phase with always break policy");
            auto operatorFusionPolicy = OperatorAtATimePolicy::create();
            return DefaultPipeliningPhase::create(operatorFusionPolicy);
        }
    };
}

LowerLogicalToPhysicalOperatorsPtr ExportPhaseFactory::createLowerLogicalQueryPlanPhase(QueryCompilerOptionsPtr options) {
    NES_DEBUG("Create default lower logical plan phase");
    auto physicalOperatorProvider = DefaultPhysicalOperatorProvider::create(options);
    return LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);
}

AddScanAndEmitPhasePtr ExportPhaseFactory::createAddScanAndEmitPhase(QueryCompilerOptionsPtr) {
    NES_DEBUG("Create add scan and emit phase");
    return AddScanAndEmitPhase::create();
}
BufferOptimizationPhasePtr ExportPhaseFactory::createBufferOptimizationPhase(QueryCompilerOptionsPtr options) {
    return NES::QueryCompilation::BufferOptimizationPhase::create(options->getOutputBufferOptimizationLevel());
}
}// namespace NES::QueryCompilation::Phases
