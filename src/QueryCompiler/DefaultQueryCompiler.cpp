#include <NodeEngine/Execution/NewExecutablePipeline.hpp>
#include <NodeEngine/Execution/NewExecutableQueryPlan.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/VizDumpHandler.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/CodeGenerationPhase.hpp>
#include <QueryCompiler/Phases/PhaseFactory.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToGeneratbaleOperatorsPhase.hpp>
#include <QueryCompiler/Phases/Translations/TranslateToPhysicalOperators.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/Logger.hpp>

namespace NES {
namespace QueryCompilation {

DefaultQueryCompiler::DefaultQueryCompiler(const QueryCompilerOptionsPtr options, const Phases::PhaseFactoryPtr phaseFactory)
    : QueryCompiler(options), translateToPhysicalOperatorsPhase(phaseFactory->createLowerLogicalQueryPlanPhase(options)),
      translateToGeneratableOperatorsPhase(phaseFactory->createLowerPipelinePlanPhase(options)),
      pipeliningPhase(phaseFactory->createPipeliningPhase(options)),
      addScanAndEmitPhase(phaseFactory->createAddScanAndEmitPhase(options)),
      codeGenerationPhase(phaseFactory->createCodeGenerationPhase(options)),
      translateToExecutableQueryPlanPhasePtr(phaseFactory->createLowerToExecutableQueryPlanPhase(options)) {}

QueryCompilerPtr DefaultQueryCompiler::create(const QueryCompilerOptionsPtr options, const Phases::PhaseFactoryPtr phaseFactory) {
    return std::make_shared<DefaultQueryCompiler>(DefaultQueryCompiler(options, phaseFactory));
}

QueryCompilationResultPtr DefaultQueryCompiler::compileQuery(QueryCompilationRequestPtr request) {

    auto queryId = request->getQueryPlan()->getQueryId();
    auto subPlanId = request->getQueryPlan()->getQuerySubPlanId();

    auto dumpContext = DumpContext::create("QueryCompilation-" + std::to_string(queryId) + "-" + std::to_string(subPlanId));
    if (request->isDumpEnabled()) {
        dumpContext->registerDumpHandler(VizDumpHandler::create());
    }

    NES_DEBUG("compile query with id: " << queryId << " subPlanId: " << subPlanId);
    auto logicalQueryPlan = request->getQueryPlan();
    dumpContext->dump("1. LogicalQueryPlan", logicalQueryPlan);

    auto physicalQueryPlan = translateToPhysicalOperatorsPhase->apply(logicalQueryPlan);
    dumpContext->dump("2. PhysicalQueryPlan", physicalQueryPlan);

    auto pipelinedQueryPlan = pipeliningPhase->apply(physicalQueryPlan);
    dumpContext->dump("3. AfterPipelinedQueryPlan", pipelinedQueryPlan);

    for (auto pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            addScanAndEmitPhase->apply(pipeline);
        }
    }
    dumpContext->dump("4. AfterAddScanAndEmitPhase", pipelinedQueryPlan);

    for (auto pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            translateToGeneratableOperatorsPhase->apply(pipeline);
        }
    }
    dumpContext->dump("5. GeneratableOperators", pipelinedQueryPlan);

    for (auto pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            codeGenerationPhase->apply(pipeline);
        }
    }
    dumpContext->dump("6. ExecutableOperatorPlan", pipelinedQueryPlan);

    auto executableQueryPlan = translateToExecutableQueryPlanPhasePtr->apply(pipelinedQueryPlan, request->getNodeEngine());
    return QueryCompilationResult::create(executableQueryPlan);
}

}// namespace QueryCompilation
}// namespace NES