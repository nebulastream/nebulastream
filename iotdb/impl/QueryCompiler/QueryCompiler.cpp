
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

QueryCompiler::QueryCompiler()  {};

QueryCompiler::QueryCompiler(QueryCompiler *queryCompiler)  {};

QueryCompilerPtr QueryCompiler::create() {
  return std::make_shared<QueryCompiler>(new QueryCompiler());
}

QueryExecutionPlanPtr QueryCompiler::compile(OperatorPtr queryPlan) {

  auto codeGenerator = createCodeGenerator();
  auto context = createPipelineContext();
  // Parse operators
  queryPlan->produce(codeGenerator, context, std::cout);
  QueryExecutionPlanPtr qep = std::make_shared<QueryExecutionPlan>();
  compilePipelineStages(qep, codeGenerator, context);
  return qep;
}

void QueryCompiler::compilePipelineStages(QueryExecutionPlanPtr queryExecutionPlan, CodeGeneratorPtr codeGenerator, PipelineContextPtr context) {
  auto executablePipeline = codeGenerator->compile(CompilerArgs(), context->code);
  if(context->hasWindow()){
    auto windowHandler = createWindowHandler(context->getWindow());
    queryExecutionPlan->addPipelineStage(createPipelineStage(queryExecutionPlan->numberOfPipelineStages(), queryExecutionPlan, executablePipeline, windowHandler));
  }else{
    queryExecutionPlan->addPipelineStage(createPipelineStage(queryExecutionPlan->numberOfPipelineStages(), queryExecutionPlan, executablePipeline));
  }
  if(context->hasNextPipeline()){
    this->compilePipelineStages(queryExecutionPlan, codeGenerator, context->getNextPipeline());
  }
}

QueryCompilerPtr createDefaultQueryCompiler(){
  return QueryCompiler::create();
}

}
