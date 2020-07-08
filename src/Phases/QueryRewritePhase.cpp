#include <Phases/QueryRewritePhase.hpp>
#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>

namespace NES{

QueryRewritePhasePtr QueryRewritePhase::create() {
    return std::make_shared<QueryRewritePhase>(QueryRewritePhase());
}

QueryRewritePhase::QueryRewritePhase() {
    filterPushDownRule = FilterPushDownRule::create();
}

QueryPlanPtr QueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    return filterPushDownRule->apply(queryPlan);
}

}