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

#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Execution/Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Listeners/QueryStatusListener.hpp>
#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Optimizer/QueryPlacementAddition/ElegantPlacementStrategy.hpp>
#include <Phases/SampleCodeGenerationPhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Phases/AddScanAndEmitPhase.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/NautilusCompilationPase.hpp>
#include <QueryCompiler/Phases/Pipelining/PipeliningPhase.hpp>
#include <QueryCompiler/Phases/Translations/LowerLogicalToPhysicalOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerPhysicalToNautilusOperators.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>

namespace NES::QueryCompilation {

class SampleCPPCodeGenerator : public NautilusQueryCompiler {
  public:
    SampleCPPCodeGenerator(QueryCompilerOptionsPtr const& options,
                           Phases::PhaseFactoryPtr const& phaseFactory,
                           bool sourceSharing)
        : NautilusQueryCompiler(options, phaseFactory, sourceSharing) {}

    QueryCompilationResultPtr compileQuery(QueryCompilation::QueryCompilationRequestPtr request) override {
        NES_INFO("Compile Query with Nautilus");
        try {
            Timer timer("DefaultQueryCompiler");
            auto queryId = request->getQueryPlan()->getQueryId();
            auto subPlanId = request->getQueryPlan()->getQuerySubPlanId();
            auto query = std::to_string(queryId) + "-" + std::to_string(subPlanId);
            // create new context for handling debug output
            auto dumpContext = DumpContext::create("QueryCompilation-" + query);

            timer.start();
            NES_DEBUG("compile query with id: {} subPlanId: {}", queryId, subPlanId);
            auto inputPlan = request->getQueryPlan();
            auto logicalQueryPlan = inputPlan->copy();
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

            // dummy buffer size to create the nautilus operators
            size_t bufferSize = 1024 * 1024;
            pipelinedQueryPlan = lowerPhysicalToNautilusOperatorsPhase->apply(pipelinedQueryPlan, bufferSize);
            timer.snapshot("AfterToNautilusPlanPhase");

            pipelinedQueryPlan = compileNautilusPlanPhase->apply(pipelinedQueryPlan);
            timer.snapshot("AfterNautilusCompilationPhase");

            for (auto& pipeline : pipelinedQueryPlan->getPipelines()) {
                if (pipeline->getQueryPlan()->getRootOperators()[0]->instanceOf<QueryCompilation::ExecutableOperator>()) {
                    auto executableOperator =
                        pipeline->getQueryPlan()->getRootOperators()[0]->as<QueryCompilation::ExecutableOperator>();
                    NES_DEBUG("executable pipeline id {}", pipeline->getPipelineId());
                    auto stage = executableOperator->getExecutablePipelineStage();
                    auto cStage = std::dynamic_pointer_cast<Runtime::Execution::CompiledExecutablePipelineStage>(stage);
                    auto dumpHelper = DumpHelper::create("", false, false);
                    Timer timer("");
                    auto ir = cStage->createIR(dumpHelper, timer);
                    auto lp = Nautilus::Backends::CPP::CPPLoweringProvider();
                    auto pipelineCPPSourceCode = lp.lower(ir);
                    auto& operatorsInPipeline = pipeline->getOperatorIds();
                    for (auto& operatorId : operatorsInPipeline) {
                        auto op = inputPlan->getOperatorWithId(operatorId);
                        if (op) {
                            op->addProperty(NES::Optimizer::ElegantPlacementStrategy::sourceCodeKey, pipelineCPPSourceCode);
                            op->addProperty("PIPELINE_ID", pipeline->getPipelineId());
                        }
                    }
                }
            }
            // The calling code is not interested in the executable plan
            // but only in the exception that is returned in the catch block.
            return QueryCompilationResult::create(nullptr, std::move(timer));
        } catch (const QueryCompilationException& exception) {
            auto currentException = std::current_exception();
            return QueryCompilationResult::create(currentException);
        }
    }
};
};// namespace NES::QueryCompilation

namespace NES::Optimizer {

SampleCodeGenerationPhase::SampleCodeGenerationPhase() {
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
    auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
    queryCompiler = std::make_shared<QueryCompilation::SampleCPPCodeGenerator>(queryCompilationOptions, phaseFactory, false);
}

SampleCodeGenerationPhasePtr SampleCodeGenerationPhase::create() {
    return std::make_shared<SampleCodeGenerationPhase>(SampleCodeGenerationPhase());
}

QueryPlanPtr SampleCodeGenerationPhase::execute(const QueryPlanPtr& plan) {
    // use query compiler to generate operator code
    // we append a property to "code" some operators
    auto request = QueryCompilation::QueryCompilationRequest::create(plan, nullptr);
    request->enableDump();
    auto result = queryCompiler->compileQuery(request);
    if (result->hasError()) {
        std::rethrow_exception(result->getError());
    }
    return plan;
}

}// namespace NES::Optimizer
