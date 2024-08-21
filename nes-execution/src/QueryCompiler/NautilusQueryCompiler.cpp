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
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::QueryCompilation
{

NautilusQueryCompiler::NautilusQueryCompiler(
    const QueryCompilation::QueryCompilerOptionsPtr& options, const Phases::PhaseFactoryPtr& phaseFactory)
    : QueryCompiler(options)
    , lowerLogicalToPhysicalOperatorsPhase(phaseFactory->createLowerLogicalQueryPlanPhase(options))
    , lowerPhysicalToNautilusOperatorsPhase(std::make_shared<LowerPhysicalToNautilusOperators>(options))
    , compileNautilusPlanPhase(std::make_shared<NautilusCompilationPhase>(options))
    , lowerToExecutableQueryPlanPhase(phaseFactory->createLowerToExecutableQueryPlanPhase())
    , pipeliningPhase(phaseFactory->createPipeliningPhase(options))
    , addScanAndEmitPhase(phaseFactory->createAddScanAndEmitPhase(options))
{
}

QueryCompilerPtr NautilusQueryCompiler::create(QueryCompilerOptionsPtr const& options, Phases::PhaseFactoryPtr const& phaseFactory)
{
    return std::make_shared<NautilusQueryCompiler>(NautilusQueryCompiler(options, phaseFactory));
}

QueryCompilation::QueryCompilationResultPtr NautilusQueryCompiler::compileQuery(QueryCompilation::QueryCompilationRequestPtr request)
{
    NES_INFO("Compile Query with Nautilus");
    Timer timer("NautilusQueryCompiler");
    auto queryId = request->getDecomposedQueryPlan()->getQueryId();
    auto query = fmt::format("{}", queryId);
    /// create new context for handling debug output
    auto dumpContext = DumpContext::create("QueryCompilation-" + query);
    dumpContext->registerDumpHandler(ConsoleDumpHandler::create(std::cout));

    timer.start();
    NES_DEBUG("compile query with id: {}", queryId);
    auto logicalQueryPlan = request->getDecomposedQueryPlan();
    dumpContext->dump("1. LogicalQueryPlan", logicalQueryPlan);
    timer.snapshot("LogicalQueryPlan");

    auto physicalQueryPlan = lowerLogicalToPhysicalOperatorsPhase->apply(logicalQueryPlan);
    dumpContext->dump("2. PhysicalQueryPlan", physicalQueryPlan);
    timer.snapshot("PhysicalQueryPlan");

    auto pipelinedQueryPlan = pipeliningPhase->apply(physicalQueryPlan);
    dumpContext->dump("3. AfterPipelinedQueryPlan", pipelinedQueryPlan);
    timer.snapshot("AfterPipelinedQueryPlan");

    addScanAndEmitPhase->apply(pipelinedQueryPlan);
    dumpContext->dump("4. AfterAddScanAndEmitPhase", pipelinedQueryPlan);
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

} /// namespace NES::QueryCompilation
