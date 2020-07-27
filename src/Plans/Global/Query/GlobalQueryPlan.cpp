#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalQueryPlan::GlobalQueryPlan() {
    root = GlobalQueryNode::createEmpty();
}

GlobalQueryPlanPtr GlobalQueryPlan::create() {
    return std::make_shared<GlobalQueryPlan>(GlobalQueryPlan());
}

void GlobalQueryPlan::addQueryPlan(QueryPlanPtr queryPlan) {

    std::string queryId = queryPlan->getQueryId();
    NES_INFO("GlobalQueryPlan: adding the query plan for query: " << queryId <<  " to the global query plan.");
    const auto rootOperators = queryPlan->getRootOperators();

    for(auto rootOperator: rootOperators){
        root->addChild()
    }
    
}

}