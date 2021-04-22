#include <QueryCompiler/QueryCompilationResult.hpp>

namespace NES {
namespace QueryCompilation {

QueryCompilationResult::QueryCompilationResult(NodeEngine::Execution::NewExecutableQueryPlanPtr executableQueryPlan): executableQueryPlan(executableQueryPlan) {}

QueryCompilationResultPtr QueryCompilationResult::create(NodeEngine::Execution::NewExecutableQueryPlanPtr qep) {
    return std::make_shared<QueryCompilationResult>(qep);
}

NodeEngine::Execution::NewExecutableQueryPlanPtr QueryCompilationResult::getExecutableQueryPlan() {
    return executableQueryPlan.value();
}

bool QueryCompilationResult::hasError() {
    return error.has_value();
}

QueryCompilationErrorPtr QueryCompilationResult::getError() {
    return error.value();
}

}// namespace QueryCompilation
}// namespace NES