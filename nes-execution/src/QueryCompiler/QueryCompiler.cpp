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
#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/Configurations/Enums/DumpMode.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPhase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <fmt/format.h>
#include <CompiledQueryPlan.hpp>

namespace NES::QueryCompilation
{

QueryCompiler::QueryCompiler(Configurations::QueryCompilerConfiguration queryCompilerConfig, const Phases::PhaseFactory& phaseFactory)
    : queryCompilerConfig(std::move(queryCompilerConfig))
    , lowerLogicalToPhysicalOperatorsPhase(phaseFactory.createLowerLogicalQueryPlanPhase(this->queryCompilerConfig))
    , lowerPhysicalToNautilusOperatorsPhase(std::make_shared<LowerPhysicalToNautilusOperators>(this->queryCompilerConfig))
    , compileNautilusPlanPhase(std::make_shared<NautilusCompilationPhase>(this->queryCompilerConfig))
    , pipeliningPhase(phaseFactory.createPipeliningPhase())
    , memoryLayoutSelectionPhase(phaseFactory.createMemoryLayoutSelectionPhase(this->queryCompilerConfig))
    , addScanAndEmitPhase(phaseFactory.createAddScanAndEmitPhase(this->queryCompilerConfig))
{
}

std::unique_ptr<Runtime::Execution::CompiledQueryPlan>
QueryCompiler::compileQuery(const std::shared_ptr<QueryCompilationRequest>& request) const
{
    NES_INFO("Compile Query with Nautilus");

    /// create new context for handling debug output
    const bool dumpToConsole = queryCompilerConfig.dumpMode == Configurations::DumpMode::CONSOLE
        || queryCompilerConfig.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE;
    const auto dumpHelper = DumpHelper("QueryCompiler", dumpToConsole, queryCompilerConfig.dumpPath.getValue());

    NES_DEBUG("compile query with id: {}", request->getDecomposedQueryPlan()->getQueryId());
    auto logicalQueryPlan = request->getDecomposedQueryPlan();
    dumpHelper.dump("1. LogicalQueryPlan", logicalQueryPlan->toString());

    const auto physicalQueryPlan = lowerLogicalToPhysicalOperatorsPhase->apply(logicalQueryPlan);
    dumpHelper.dump("2. PhysicalQueryPlan", physicalQueryPlan->toString());

    auto pipelinedQueryPlan = pipeliningPhase->apply(physicalQueryPlan);
    dumpHelper.dump("3. AfterPipelinedQueryPlan", pipelinedQueryPlan->toString());

    pipelinedQueryPlan = memoryLayoutSelectionPhase->apply(pipelinedQueryPlan);
    dumpHelper.dump("4. AfterMemoryLayoutSelectionPhase", pipelinedQueryPlan->toString());

    pipelinedQueryPlan = addScanAndEmitPhase->apply(pipelinedQueryPlan);
    dumpHelper.dump("5. AfterAddScanAndEmitPhase", pipelinedQueryPlan->toString());

    auto bufferSize = request->getBufferSize();
    pipelinedQueryPlan = lowerPhysicalToNautilusOperatorsPhase->apply(pipelinedQueryPlan, bufferSize);

    pipelinedQueryPlan = compileNautilusPlanPhase->apply(pipelinedQueryPlan);

    auto executableQueryPlan = LowerToExecutableQueryPlanPhase::apply(pipelinedQueryPlan);
    return executableQueryPlan;
}
}
