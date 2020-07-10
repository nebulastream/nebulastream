#include <Catalogs/QueryCatalog.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Operators/OperatorJsonUtil.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
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

json::value QueryService::getQueryPlanAsJson(std::string queryId) {

    NES_INFO("QueryService: Get the registered query");
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQuery(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("QueryString: Unable to find entry for the query with id " << queryId);
    }
    NES_INFO("QueryService: Getting the json representation of the query plan");
    return OperatorJsonUtil::getBasePlan(queryCatalogEntry->getQueryPlan());
}

//string QueryService::addQuery(const string queryString, const string strategy) {
//    NES_DEBUG("NesCoordinator:addQuery queryString=" << queryString << " strategy=" << strategy);
//
//    NES_DEBUG("NesCoordinator:addQuery: register query");
//    string queryId = registerQuery(queryString, strategy);
//    if (queryId != "") {
//        NES_DEBUG("NesCoordinator:addQuery: register query successful");
//    } else {
//        NES_ERROR("NesCoordinator:addQuery: register query failed");
//        return "";
//    }
//
//    NES_DEBUG("NesCoordinator:addQuery: deploy query");
//    bool successDeploy = deployQuery(queryId);
//    if (successDeploy) {
//        NES_DEBUG("NesCoordinator:addQuery: deploy query successful");
//    } else {
//        NES_ERROR("NesCoordinator:addQuery: deploy query failed");
//        return "";
//    }
//
//    NES_DEBUG("NesCoordinator:addQuery: start query");
//    bool successStart = startQuery(queryId);
//    if (successStart) {
//        NES_DEBUG("NesCoordinator:addQuery: start query successful");
//    } else {
//        NES_ERROR("NesCoordinator:addQuery: start query failed");
//        return "";
//    }
//
//    return queryId;
//}

bool QueryService::removeQuery(const std::string queryId) {
    NES_DEBUG("NesCoordinator:removeQuery queryId=" << queryId);

    NES_DEBUG("NesCoordinator:removeQuery: stop query");
    bool successStop = stopQuery(queryId);
    if (successStop) {
        NES_DEBUG("NesCoordinator:removeQuery: stop query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: stop query failed");
        return false;
    }

    NES_DEBUG("NesCoordinator:removeQuery: undeploy query");
    bool successUndeploy = undeployQuery(queryId);
    if (successUndeploy) {
        NES_DEBUG("NesCoordinator:removeQuery: undeploy query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: undeploy query failed");
        return false;
    }

    NES_DEBUG("NesCoordinator:removeQuery: unregister query");
    bool successUnregister = unregisterQuery(queryId);
    if (successUnregister) {
        NES_DEBUG("NesCoordinator:removeQuery: unregister query successful");
    } else {
        NES_ERROR("NesCoordinator:removeQuery: unregister query failed");
        return false;
    }

    return true;
}

bool QueryService::unregisterQuery(const std::string queryId) {
    NES_DEBUG("NesCoordinator:unregisterQuery queryId=" << queryId);
    return queryCatalog->deleteQuery(queryId);
}

bool QueryService::undeployQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator::undeployQuery queryId=" << queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("NesCoordinator: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        string nesNodeIp = executionNode->getNesNode()->getIp();
        NES_DEBUG("NESCoordinator::undeployQuery query at execution node with id=" << executionNode->getId() << " and IP=" << nesNodeIp);
        bool success = workerRPCClient->unregisterQuery(nesNodeIp, queryId);
        if (success) {
            NES_DEBUG("NESCoordinator::undeployQuery  query " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("NESCoordinator::undeployQuery  " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    return true;
}

bool QueryService::startQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator::startQuery queryId=" << queryId);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("NesCoordinator: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        string nesNodeIp = executionNode->getNesNode()->getIp();
        NES_DEBUG("NesCoordinator::startQuery at execution node with id=" << executionNode->getId() << " and IP=" << nesNodeIp);
        bool success = workerRPCClient->startQuery(nesNodeIp, queryId);
        if (success) {
            NES_DEBUG("NesCoordinator::startQuery " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("NesCoordinator::startQuery " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    return true;
}

bool QueryService::stopQuery(std::string queryId) {
    NES_DEBUG("NesCoordinator:stopQuery queryId=" << queryId);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("NesCoordinator: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        string nesNodeIp = executionNode->getNesNode()->getIp();
        NES_DEBUG("NESCoordinator::stopQuery at execution node with id=" << executionNode->getId() << " and IP=" << nesNodeIp);
        bool success = workerRPCClient->stopQuery(nesNodeIp, queryId);
        if (success) {
            NES_DEBUG("NESCoordinator::stopQuery " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("NESCoordinator::stopQuery " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    return true;
}

}// namespace NES
