#include <QueryCompiler/QueryCompilationResult.hpp>

namespace NES {
namespace QueryCompilation {

QueryCompilationResult::QueryCompilationResult(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan): executableQueryPlan(executableQueryPlan) {}

QueryCompilationResultPtr QueryCompilationResult::create(NodeEngine::Execution::NewExecutableQueryPlanPtr qep) {
    return std::make_shared<QueryCompilationResult>(qep);
}

NodeEngine::Execution::NewExecutableQueryPlanPtr QueryCompilationResult::getExecutableQueryPlan() {
    return executableQueryPlan;
}

}// namespace QueryCompilation
}// namespace NES