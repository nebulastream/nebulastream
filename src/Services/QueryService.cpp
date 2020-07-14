#include <Catalogs/QueryCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
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

//TODO: Add mutex to make certain blocks synchronized

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
    QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQuery(queryId);
    if (!queryCatalogEntry) {
        NES_ERROR("QueryString: Unable to find entry for the query with id " << queryId);
    }
    NES_INFO("QueryService: Getting the json representation of the query plan");
    return OperatorJsonUtil::getBasePlan(queryCatalogEntry->getQueryPlan());
}

bool QueryService::deployAndStartQuery(std::string queryId) {
    NES_DEBUG("QueryService: deploy the query");
    bool successDeploy = deployQuery(queryId);
    if (successDeploy) {
        NES_DEBUG("QueryService: deployment for query " + queryId + " successful");
    } else {
        NES_ERROR("QueryService: Failed to deploy query " + queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(queryId);
    if (successStart) {
        NES_DEBUG("QueryService: Successfully started deployed query " + queryId);
    } else {
        NES_ERROR("QueryService: Failed to start the deployed query " + queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
    return true;
}

bool QueryService::stopAndUndeployQuery(const std::string queryId) {
    NES_DEBUG("QueryService::stopAndUndeployQuery : queryId=" << queryId);

    //TODO: Throw QueryNotFoundException for non-existing queryId.
    const QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQuery(queryId);
    QueryStatus currentStatus = queryCatalogEntry->getQueryStatus();
    if (currentStatus == QueryStatus::Scheduling) {

        //FIXME: I am not sure if this is secure ... we might end up in a situation that query is marked to be stopped but it is still running in the system.
        // Or the query is stopped but status is Marked For Stop.
        NES_INFO("QueryService: Found the query " + queryId + " is being scheduled currently. Marking the query to be stopped later.");
        queryCatalog->markQueryAs(queryId, QueryStatus::MarkedForStop);
        return true;
    } else if (currentStatus == QueryStatus::Running || currentStatus == QueryStatus::MarkedForStop) {

        NES_DEBUG("QueryService:removeQuery: stop query");
        bool successStop = stopQuery(queryId);
        if (successStop) {
            NES_DEBUG("QueryService:removeQuery: stop query successful");
        } else {
            NES_ERROR("QueryService:removeQuery: stop query failed");
            return false;
        }

        NES_DEBUG("QueryService:removeQuery: undeploy query");
        bool successUndeploy = undeployQuery(queryId);
        if (successUndeploy) {
            NES_DEBUG("QueryService:removeQuery: undeploy query successful");
            return true;
        } else {
            NES_ERROR("QueryService:removeQuery: undeploy query failed");
            return false;
        }
    } else {
        NES_WARNING("QueryService: Trying to undeploy and stop the query " + queryId + " in " + queryCatalogEntry->getQueryString() + " status.");
        return false;
    }
}

bool QueryService::deployQuery(std::string queryId) {

    NES_DEBUG("QueryService::deployQuery queryId=" << queryId);
    NES_DEBUG("QueryService: preparing for Deployment by adding port information");
    if (!queryDeployer->prepareForDeployment(queryId)) {
        NES_ERROR("QueryService: Failed to prepare for Deployment by adding port information");
        return false;
    }

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryService: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryService::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
        if (!querySubPlan) {
            NES_WARNING("QueryService : unable to find query sub plan with id " << queryId);
            return false;
        }
        //FIXME: we are considering only one root operator
        OperatorNodePtr rootOperator = querySubPlan->getRootOperators()[0];
        std::string nesNodeIp = executionNode->getNesNode()->getIp();
        NES_DEBUG("QueryService:deployQuery: " << queryId << " to " << nesNodeIp);
        bool success = workerRPCClient->registerQuery(nesNodeIp, queryId, rootOperator);
        if (success) {
            NES_DEBUG("QueryService:deployQuery: " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("QueryService:deployQuery: " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
    NES_INFO("QueryService: Finished deploying execution plan for query with Id " << queryId);
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

    NES_DEBUG("QueryService:stopQuery queryId=" << queryId);
    const QueryCatalogEntryPtr queryCatalogEntry = queryCatalog->getQuery(queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryService: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        string nesNodeIp = executionNode->getNesNode()->getIp();
        NES_DEBUG("QueryService::stopQuery at execution node with id=" << executionNode->getId() << " and IP=" << nesNodeIp);
        bool success = workerRPCClient->stopQuery(nesNodeIp, queryId);
        if (success) {
            NES_DEBUG("QueryService::stopQuery " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("QueryService::stopQuery " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    return true;
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

}// namespace NES
