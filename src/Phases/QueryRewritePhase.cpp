#include <Optimizer/QueryRewrite/FilterPushDownRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Phases/QueryRewritePhase.hpp>

namespace NES {

QueryRewritePhasePtr QueryRewritePhase::create(StreamCatalogPtr streamCatalog) {
    return std::make_shared<QueryRewritePhase>(QueryRewritePhase(streamCatalog));
}

QueryRewritePhase::QueryRewritePhase(StreamCatalogPtr streamCatalog) {
    filterPushDownRule = FilterPushDownRule::create();
    logicalSourceExpansionRule = LogicalSourceExpansionRule::create(streamCatalog);
}

QueryRewritePhase::~QueryRewritePhase() {
    NES_DEBUG("~QueryRewritePhase()");
}

QueryPlanPtr QueryRewritePhase::execute(QueryPlanPtr queryPlan) {
    queryPlan = filterPushDownRule->apply(queryPlan);
    return logicalSourceExpansionRule->apply(queryPlan);
}

}// namespace NES