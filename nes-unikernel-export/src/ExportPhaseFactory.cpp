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
