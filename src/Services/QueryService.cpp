#include <Catalogs/QueryCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog) : queryCatalog(queryCatalog) {
    NES_DEBUG("QueryService()");
}

std::string QueryService::validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName) {

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
    queryCatalog->registerAndQueueAddRequest(queryString, queryPlan, placementStrategyName);
    return queryId;
}

bool QueryService::validateAndQueueStopRequest(std::string queryId) {

    NES_INFO("QueryService : stopping query " + queryId);
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + queryId + " in query catalog.");
    }
    return queryCatalog->queueStopRequest(queryId);
}

json::value QueryService::getQueryPlanAsJson(std::string queryId) {

    NES_INFO("QueryService: Get the registered query");
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("QueryString: Unable to find entry for the query with id " << queryId);
    }
    NES_INFO("QueryService: Getting the json representation of the query plan");
    return OperatorJsonUtil::getBasePlan(queryCatalogEntry->getQueryPlan());
}

}// namespace NES
