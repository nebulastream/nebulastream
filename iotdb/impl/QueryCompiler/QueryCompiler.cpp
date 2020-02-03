
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
  auto pipelineStages = std::vector<PipelineStagePtr>();
  compilePipelineStages(pipelineStages, codeGenerator, context);

  QueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(pipelineStages));
  return qep;
}

void QueryCompiler::compilePipelineStages(std::vector<PipelineStagePtr> pipelineStages, CodeGeneratorPtr codeGenerator, PipelineContextPtr context) {
  pipelineStages.push_back(codeGenerator->compile(CompilerArgs(), context->code));
  if(context->hasNextPipeline()){
    this->compilePipelineStages(pipelineStages, codeGenerator, context->getNextPipeline());
  }
}

QueryCompilerPtr createDefaultQueryCompiler(){
  return QueryCompiler::create();
}

}
