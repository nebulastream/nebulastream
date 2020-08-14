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
#include <WorkQueues/QueryRequestQueue.hpp>

namespace NES {

QueryService::QueryService(QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue) : queryCatalog(queryCatalog), queryRequestQueue(queryRequestQueue) {
    NES_DEBUG("QueryService()");
}

uint64_t QueryService::validateAndQueueAddRequest(std::string queryString, std::string placementStrategyName) {

    NES_INFO("QueryService: Validating and registering the user query.");
    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        NES_ERROR("QueryService: Unknown placement strategy name: " + placementStrategyName);
        throw InvalidArgumentException("placementStrategyName", placementStrategyName);
    }
    NES_INFO("QueryService: Parsing and converting user query string");
    QueryPtr query = UtilityFunctions::createQueryFromCodeString(queryString);
//    uint64_t queryId = UtilityFunctions::getNextQueryId();
    uint64_t queryId = 777777;
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    NES_INFO("QueryService: Queuing the query for the execution");
    QueryCatalogEntryPtr entry = queryCatalog->addNewQueryRequest(queryString, queryPlan, placementStrategyName);
    if (entry) {
        queryRequestQueue->add(entry);
        return queryId;
    } else {
        throw Exception("QueryService: unable to create query catalog entry");
    }
}

bool QueryService::validateAndQueueStopRequest(uint64_t queryId) {

    NES_INFO("QueryService : stopping query " + queryId);
    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryService: Unable to find query with id " + std::to_string(queryId) + " in query catalog.");
    }
    QueryCatalogEntryPtr entry = queryCatalog->addQueryStopRequest(queryId);
    if (entry) {
        return queryRequestQueue->add(entry);
    }
    return false;
}

json::value QueryService::getQueryPlanAsJson(uint64_t queryId) {

    NES_INFO("QueryService: Get the registered query");
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQueryCatalogEntry(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("QueryString: Unable to find entry for the query with id " << queryId);
    }
    NES_INFO("QueryService: Getting the json representation of the query plan");
    return OperatorJsonUtil::getBasePlan(queryCatalogEntry->getQueryPlan());
}

}// namespace NES
