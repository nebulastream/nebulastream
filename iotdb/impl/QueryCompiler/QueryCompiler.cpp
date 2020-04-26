
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

QueryCompiler::QueryCompiler() {};

QueryCompiler::QueryCompiler(QueryCompiler* queryCompiler) {};

QueryCompilerPtr QueryCompiler::create() {
    return std::make_shared<QueryCompiler>(new QueryCompiler());
}

QueryExecutionPlanPtr QueryCompiler::compile(OperatorPtr queryPlan) {

    auto codeGenerator = createCodeGenerator();
    auto context = createPipelineContext();
    queryPlan->produce(codeGenerator, context, std::cout);
    QueryExecutionPlanPtr qep = std::make_shared<GeneratedQueryExecutionPlan>();
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
        auto windowHandler = createWindowHandler(context->getWindow(), queryExecutionPlan->getDispatcher());
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

QueryCompilerPtr createDefaultQueryCompiler() {
    return QueryCompiler::create();
}

}
