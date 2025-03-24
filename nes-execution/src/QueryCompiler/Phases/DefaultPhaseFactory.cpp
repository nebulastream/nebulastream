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
#include <memory>
#include <utility>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/FixMemoryLayoutSelectionPhase.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/MemoryLayoutSelectionPhase.hpp>
#include <QueryCompiler/Phases/MemoryLayoutSelection/NoneMemoryLayoutSelectionPhase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/DefaultPipeliningPhase.hpp>
#include <QueryCompiler/Phases/Pipelining/FuseNonPipelineBreakerPolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/OperatorAtATimePolicy.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/DefaultPhysicalOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation::Phases
{

std::shared_ptr<PhaseFactory> DefaultPhaseFactory::create()
{
    return std::make_shared<DefaultPhaseFactory>();
}

std::shared_ptr<PipeliningPhase> DefaultPhaseFactory::createPipeliningPhase() const
{
    NES_DEBUG("Create pipelining phase with fuse policy");
    const auto operatorFusionPolicy = FuseNonPipelineBreakerPolicy::create();
    return DefaultPipeliningPhase::create(operatorFusionPolicy);
}

std::shared_ptr<LowerLogicalToPhysicalOperators>
DefaultPhaseFactory::createLowerLogicalQueryPlanPhase(Configurations::QueryCompilerConfiguration queryCompilerConfig) const
{
    NES_DEBUG("Create default lower logical plan phase");
    const auto physicalOperatorProvider = std::make_shared<DefaultPhysicalOperatorProvider>(std::move(queryCompilerConfig));
    return LowerLogicalToPhysicalOperators::create(physicalOperatorProvider);
}

std::shared_ptr<MemoryLayoutSelectionPhase>
DefaultPhaseFactory::createMemoryLayoutSelectionPhase(Configurations::QueryCompilerConfiguration queryCompilerConfig) const
{
    NES_DEBUG("Create memory layout selection phase");
    switch (queryCompilerConfig.memorySelectionPhase)
    {
        case Configurations::MemorySelectionPhaseType::FIXED:
            return FixMemoryLayoutSelectionPhase::create(queryCompilerConfig.memoryLayoutType);
        case Configurations::MemorySelectionPhaseType::NONE:
            return NoneMemoryLayoutSelectionPhase::create();
    }
}

std::shared_ptr<AddScanAndEmitPhase> DefaultPhaseFactory::createAddScanAndEmitPhase(Configurations::QueryCompilerConfiguration) const
{
    NES_DEBUG("Create add scan and emit phase");
    return AddScanAndEmitPhase::create();
}

}
