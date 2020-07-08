#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog) : queryCatalog(queryCatalog) {
    NES_DEBUG("QueryService()");
}

std::string QueryService::validateAndRegisterQuery(std::string queryString, std::string placementStrategyName) {

    NES_INFO("QueryService: Validating and registering the user query.");
    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
        throw InvalidArgumentException("BasePlacementStrategy: Unknown placement strategy name", "placementStrategyName", placementStrategyName);
    }
    NES_INFO("QueryService: Parsing and converting user query string");
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
    std::string queryId = UtilityFunctions::generateIdString();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    NES_INFO("QueryService: Queuing the query for the execution");
    queryCatalog->registerAndAddToSchedulingQueue(queryString, queryPlan, placementStrategyName);
    return queryId;
}

json::value QueryService::getQueryPlanForQueryId(std::string userQuery) {
    //build query from string
    QueryPtr query = getQueryFromQueryString(userQuery);
    //build the query plan
    const json::value& basePlan = OperatorJsonUtil::getBasePlan(query->getQueryPlan());
    return basePlan;
}

QueryPtr QueryService::getQueryFromQueryString(std::string userQuery) {
    return UtilityFunctions::createQueryFromCodeString(userQuery);
}

}// namespace NES
