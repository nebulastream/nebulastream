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

#include <utility>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation
{

NautilusQueryCompiler::NautilusQueryCompiler(
    std::shared_ptr<QueryCompilerOptions> options, std::shared_ptr<Phases::PhaseFactory> phaseFactory)
    : QueryCompiler(options)
    , lowerLogicalToPhysicalOperatorsPhase(phaseFactory->createLowerLogicalQueryPlanPhase(options))
    , lowerPhysicalToNautilusOperatorsPhase(std::make_shared<LowerPhysicalToNautilusOperators>(options))
    , compileNautilusPlanPhase(std::make_shared<NautilusCompilationPhase>(options))
    , lowerToExecutableQueryPlanPhase(phaseFactory->createLowerToExecutableQueryPlanPhase())
    , pipeliningPhase(phaseFactory->createPipeliningPhase())
    , addScanAndEmitPhase(phaseFactory->createAddScanAndEmitPhase(options))
{
}

std::shared_ptr<QueryCompilationResult> NautilusQueryCompiler::compileQuery(QueryCompilationRequestPtr request, QueryId queryId)
{
    NES_INFO("Compile Query with Nautilus");
    /// For now we have to override the id here as it should not be set by the client
    request->getDecomposedQueryPlan()->setQueryId(queryId);

    Timer timer("NautilusQueryCompiler");
    /// Uncomment these dumb informations for debugging purposes. They are be quite intrusive.
    auto query = fmt::format("{}", queryId);
    /// create new context for handling debug output
    bool dumpToFile = options->dumpMode != DumpMode::CONSOLE;
    bool dumpToConsole = options->dumpMode != DumpMode::FILE;
    auto dumpHelper = DumpHelper("QueryCompiler", dumpToConsole, dumpToFile, options->dumpPath);

    timer.start();
    NES_DEBUG("compile query with id: {}", queryId);
    auto logicalQueryPlan = request->getDecomposedQueryPlan();
    dumpHelper.dump("1. LogicalQueryPlan", logicalQueryPlan->toString());
    timer.snapshot("LogicalQueryPlan");

    auto physicalQueryPlan = lowerLogicalToPhysicalOperatorsPhase->apply(logicalQueryPlan);
    dumpHelper.dump("2. PhysicalQueryPlan", physicalQueryPlan->toString());
    timer.snapshot("PhysicalQueryPlan");

    auto pipelinedQueryPlan = pipeliningPhase->apply(physicalQueryPlan);
    dumpHelper.dump("3. AfterPipelinedQueryPlan", pipelinedQueryPlan->toString());
    timer.snapshot("AfterPipelinedQueryPlan");

    addScanAndEmitPhase->apply(pipelinedQueryPlan);
    dumpHelper.dump("4. AfterAddScanAndEmitPhase", pipelinedQueryPlan->toString());
    timer.snapshot("AfterAddScanAndEmitPhase");
    auto nodeEngine = request->getNodeEngine();
    auto bufferSize = nodeEngine->getQueryManager()->getBufferManager()->getBufferSize();
    pipelinedQueryPlan = lowerPhysicalToNautilusOperatorsPhase->apply(pipelinedQueryPlan, bufferSize);
    timer.snapshot("AfterToNautilusPlanPhase");

    pipelinedQueryPlan = compileNautilusPlanPhase->apply(pipelinedQueryPlan);
    timer.snapshot("AfterNautilusCompilationPhase");

    auto executableQueryPlan = lowerToExecutableQueryPlanPhase->apply(pipelinedQueryPlan, request->getNodeEngine());
    return QueryCompilationResult::create(executableQueryPlan, std::move(timer));
}
}
