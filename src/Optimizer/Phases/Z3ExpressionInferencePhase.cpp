#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/Phases/Z3ExpressionInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace NES::Optimizer {

Z3ExpressionInferencePhase::Z3ExpressionInferencePhase() {
    context = std::make_shared<z3::context>();
    NES_DEBUG("Z3ExpressionInferencePhase()");
}

Z3ExpressionInferencePhase::~Z3ExpressionInferencePhase() {
    NES_DEBUG("~Z3ExpressionInferencePhase()");
}

Z3ExpressionInferencePhasePtr Z3ExpressionInferencePhase::create() {
    return std::make_shared<Z3ExpressionInferencePhase>(Z3ExpressionInferencePhase());
}

void Z3ExpressionInferencePhase::execute(QueryPlanPtr queryPlan) {
    NES_INFO("Z3ExpressionInferencePhase: computing Z3 expressions for the query " << queryPlan->getQueryId());
    auto sinkOperators = queryPlan->getRootOperators();
    for (auto sinkOperator : sinkOperators) {
        sinkOperator->as<LogicalOperatorNode>()->inferZ3Expression(context);
    }
}

}// namespace NES::Optimizer
