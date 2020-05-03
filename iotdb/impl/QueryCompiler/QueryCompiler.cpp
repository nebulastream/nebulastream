#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <NodeEngine/QueryManager.hpp>
namespace NES {

QueryCompiler::QueryCompiler() {};

QueryCompiler::QueryCompiler(QueryCompiler* queryCompiler) {};

QueryCompilerPtr QueryCompiler::create() {
    return std::make_shared<QueryCompiler>(new QueryCompiler());
}

void QueryCompiler::setQueryManager(QueryManagerPtr queryManager) {
    this->queryManager = queryManager;
}
void QueryCompiler::setBufferManager(BufferManagerPtr bufferManager)
{
    this->bufferManager = bufferManager;
}

QueryExecutionPlanPtr QueryCompiler::compile(OperatorPtr queryPlan) {

    auto codeGenerator = createCodeGenerator();
    auto context = createPipelineContext();
    queryPlan->produce(codeGenerator, context, std::cout);
    QueryExecutionPlanPtr qep = std::make_shared<GeneratedQueryExecutionPlan>();
    qep->setQueryManager(queryManager);
    qep->setBufferManager(bufferManager);
    compilePipelineStages(qep, codeGenerator, context);
    return qep;
}

void QueryCompiler::compilePipelineStages(QueryExecutionPlanPtr queryExecutionPlan,
                                          CodeGeneratorPtr codeGenerator,
                                          PipelineContextPtr context) {
    if (context->hasNextPipeline()) {
        this->compilePipelineStages(queryExecutionPlan, codeGenerator, context->getNextPipeline());
    }
    auto executablePipeline = codeGenerator->compile(CompilerArgs(), context->code);
    if (context->hasWindow()) {
        auto windowHandler = createWindowHandler(context->getWindow(),
                                                 queryExecutionPlan->getQueryManager(), queryExecutionPlan->getBufferManager());
        queryExecutionPlan->appendsPipelineStage(createPipelineStage(queryExecutionPlan->numberOfPipelineStages(),
                                                                     queryExecutionPlan,
                                                                     executablePipeline,
                                                                     windowHandler));
    } else {
        queryExecutionPlan->appendsPipelineStage(createPipelineStage(queryExecutionPlan->numberOfPipelineStages(),
                                                                     queryExecutionPlan,
                                                                     executablePipeline));
    }

}

QueryCompilerPtr createDefaultQueryCompiler(QueryManagerPtr queryManager) {
    auto q = QueryCompiler::create();
    q->setQueryManager(queryManager);
    return q;
}

}