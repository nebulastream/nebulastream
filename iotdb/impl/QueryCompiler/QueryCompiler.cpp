
#include <QueryCompiler/QueryCompiler.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
namespace iotdb {

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
  auto stage = codeGenerator->compile(CompilerArgs());
  QueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(stage));
  return qep;
}

QueryCompilerPtr createDefaultQueryCompiler(){
  return QueryCompiler::create();
}

}
