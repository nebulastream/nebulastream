#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Phases/Z3ExpressionInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <z3++.h>

namespace z3 {
class context;
class expr;
}// namespace z3

namespace NES::Optimizer {

Z3ExpressionInferencePhase::Z3ExpressionInferencePhase() {
    NES_DEBUG("Z3ExpressionInferencePhase()");
}

Z3ExpressionInferencePhase::~Z3ExpressionInferencePhase() {
    NES_DEBUG("~Z3ExpressionInferencePhase()");
}

Z3ExpressionInferencePhasePtr Z3ExpressionInferencePhase::create() {
    return std::make_shared<Z3ExpressionInferencePhase>(Z3ExpressionInferencePhase());
}

void Z3ExpressionInferencePhase::execute(QueryPlanPtr queryPlan) {
    z3::context context;
    auto sinkOperators = queryPlan->getRootOperators();
    for (auto sinkOperator : sinkOperators) {
        sinkOperator->as<LogicalOperatorNode>()->getZ3Expression(context);
    }
}



}// namespace NES::Optimizer
