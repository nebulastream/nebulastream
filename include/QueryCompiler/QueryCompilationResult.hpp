#ifndef NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
#define NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES {
namespace QueryCompilation {

class QueryCompilationResult {
  public:
    QueryCompilationResult(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan);
    static QueryCompilationResultPtr create(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan);

    NodeEngine::Execution::NewExecutableQueryPlanPtr getExecutableQueryPlan();
  private:
    NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan;
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_QUERYCOMPILATIONRESULT_HPP_
