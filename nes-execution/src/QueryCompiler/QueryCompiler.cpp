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
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <fmt/format.h>

namespace NES::QueryCompilation
{

QueryCompiler::QueryCompiler(Configurations::QueryCompilerConfiguration queryCompilerConfig, const Phases::PhaseFactory& phaseFactory)
    : queryCompilerConfig(std::move(queryCompilerConfig))
    , lowerLogicalToPhysicalOperatorsPhase(phaseFactory.createLowerLogicalQueryPlanPhase(this->queryCompilerConfig))
    , lowerPhysicalToNautilusOperatorsPhase(std::make_shared<LowerPhysicalToNautilusOperators>(this->queryCompilerConfig))
    , compileNautilusPlanPhase(std::make_shared<NautilusCompilationPhase>(this->queryCompilerConfig))
    , pipeliningPhase(phaseFactory.createPipeliningPhase())
    , addScanAndEmitPhase(phaseFactory.createAddScanAndEmitPhase(queryCompilerConfig))
{
}

QueryCompilationResult QueryCompiler::compileQuery(const std::shared_ptr<QueryCompilationRequest>& request) const
{
    NES_INFO("Compile Query with Nautilus");

    Timer timer("QueryCompiler");
    /// Uncomment these dump informations for debugging purposes. They can be quite intrusive.
    /// create new context for handling debug output
    const bool dumpToFile = queryCompilerConfig.dumpMode == Configurations::DumpMode::FILE
        || queryCompilerConfig.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE;
    const bool dumpToConsole = queryCompilerConfig.dumpMode == Configurations::DumpMode::CONSOLE
        || queryCompilerConfig.dumpMode == Configurations::DumpMode::FILE_AND_CONSOLE;
    const auto dumpHelper = DumpHelper("QueryCompiler", dumpToConsole, dumpToFile, queryCompilerConfig.dumpPath.getValue());

    timer.start();
    NES_DEBUG("compile query with id: {}", request->getDecomposedQueryPlan()->getQueryId());
    const auto logicalQueryPlan = request->getDecomposedQueryPlan();
    dumpHelper.dump("1. LogicalQueryPlan", logicalQueryPlan->toString());
    timer.snapshot("LogicalQueryPlan");

    const auto physicalQueryPlan = lowerLogicalToPhysicalOperatorsPhase->apply(logicalQueryPlan);
    dumpHelper.dump("2. PhysicalQueryPlan", physicalQueryPlan->toString());
    timer.snapshot("PhysicalQueryPlan");

    auto pipelinedQueryPlan = pipeliningPhase->apply(physicalQueryPlan);
    dumpHelper.dump("3. AfterPipelinedQueryPlan", pipelinedQueryPlan->toString());
    timer.snapshot("AfterPipelinedQueryPlan");

    addScanAndEmitPhase->apply(pipelinedQueryPlan);
    dumpHelper.dump("4. AfterAddScanAndEmitPhase", pipelinedQueryPlan->toString());
    timer.snapshot("AfterAddScanAndEmitPhase");
    const auto bufferSize = request->getBufferSize();
    pipelinedQueryPlan = lowerPhysicalToNautilusOperatorsPhase->apply(pipelinedQueryPlan, bufferSize);
    timer.snapshot("AfterToNautilusPlanPhase");

    pipelinedQueryPlan = compileNautilusPlanPhase->apply(pipelinedQueryPlan);
    timer.snapshot("AfterNautilusCompilationPhase");

    auto executableQueryPlan = LowerToExecutableQueryPlanPhase::apply(pipelinedQueryPlan);
    return {std::move(executableQueryPlan), std::move(timer)};
}
}
