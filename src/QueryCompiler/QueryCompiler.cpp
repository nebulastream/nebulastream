#include <NodeEngine/QueryManager.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
namespace NES {

QueryCompiler::QueryCompiler(){};

QueryCompiler::QueryCompiler(QueryCompiler* queryCompiler){};

QueryCompilerPtr QueryCompiler::create() {
    return std::make_shared<QueryCompiler>(new QueryCompiler());
}

void QueryCompiler::setQueryManager(QueryManagerPtr queryManager) {
    this->queryManager = queryManager;
}
void QueryCompiler::setBufferManager(BufferManagerPtr bufferManager) {
    this->bufferManager = bufferManager;
}

QueryExecutionPlanPtr QueryCompiler::compile(OperatorPtr queryPlan) {

    auto codeGenerator = createCodeGenerator();
    auto context = createPipelineContext();
    queryPlan->produce(codeGenerator, context, std::cout);
    QueryExecutionPlanPtr qep = GeneratedQueryExecutionPlan::create();
    qep->setQueryManager(queryManager);
    qep->setBufferManager(bufferManager);
    compilePipelineStages(qep, codeGenerator, context);
    return qep;
}

std::shared_ptr<PipelineStage> QueryCompiler::compilePipelineStages(QueryExecutionPlanPtr queryExecutionPlan,
                                                                    CodeGeneratorPtr codeGenerator,
                                                                    PipelineContextPtr context) {
    PipelineStagePtr pipelineStage;

    //if this stage has any children we need to set up the connection now
    std::vector<PipelineStagePtr> childStages;
    auto nextPipelines = context->getNextPipelines();
    for (auto nextPipeline : nextPipelines) {
        childStages.push_back(this->compilePipelineStages(queryExecutionPlan, codeGenerator, nextPipeline));
    }
    auto executablePipeline = codeGenerator->compile(CompilerArgs(), context->code);

    if (context->hasWindow()) {
        auto windowHandler = createWindowHandler(context->getWindow(),
                                                 queryExecutionPlan->getQueryManager(), queryExecutionPlan->getBufferManager());
        pipelineStage = createPipelineStage(queryExecutionPlan->numberOfPipelineStages(),
                                        queryExecutionPlan,
                                        executablePipeline,
                                        windowHandler);
        queryExecutionPlan->appendPipelineStage(pipelineStage);
    } else {
        pipelineStage = createPipelineStage(queryExecutionPlan->numberOfPipelineStages(),
                                        queryExecutionPlan,
                                        executablePipeline);
        queryExecutionPlan->appendPipelineStage(pipelineStage);
    }

    for (auto childStage : childStages) {
        childStage->setNextStage(pipelineStage);
    }

    return pipelineStage;
}

QueryCompilerPtr createDefaultQueryCompiler(QueryManagerPtr queryManager) {
    auto q = QueryCompiler::create();
    q->setQueryManager(queryManager);
    return q;
}

}// namespace NES