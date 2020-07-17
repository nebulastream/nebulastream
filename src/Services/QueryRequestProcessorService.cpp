#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/InvalidQueryStatusException.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryNotFoundException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryRequestProcessorService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryRequestProcessorService::QueryRequestProcessorService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan,
                                                           QueryCatalogPtr queryCatalog, StreamCatalogPtr streamCatalog, WorkerRPCClientPtr workerRpcClient,
                                                           QueryDeployerPtr queryDeployer)
    : queryProcessorRunning(true), globalExecutionPlan(globalExecutionPlan), queryCatalog(queryCatalog), workerRPCClient(workerRpcClient), queryDeployer(queryDeployer) {

    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
}

void QueryRequestProcessorService::start() {

    try {
        while (queryProcessorRunning) {
            if (queryCatalog->isNewRequestAvailable()) {
                const std::vector<QueryCatalogEntry> queryCatalogEntryBatch = queryCatalog->getQueriesToSchedule();
                NES_INFO("QueryProcessingService: Found " << queryCatalogEntryBatch.size() << " query requests to schedule");
                //process the queries using query-at-a-time model
                for (auto queryCatalogEntry : queryCatalogEntryBatch) {

                    auto queryPlan = queryCatalogEntry.getQueryPlan();
                    std::string queryId = queryPlan->getQueryId();
                    try {
                        if (queryCatalogEntry.getQueryStatus() == QueryStatus::MarkedForStop) {
                            NES_INFO("QueryProcessingService: Request received for stopping the query " + queryId);
                            bool successful = stopAndUndeployQuery(queryId);
                            if (!successful) {
                                throw QueryUndeploymentException("Unable to stop the query " + queryId);
                            }
                            queryCatalog->markQueryAs(queryId, QueryStatus::Stopped);
                        } else if (queryCatalogEntry.getQueryStatus() == QueryStatus::Registered) {
                            NES_INFO("QueryProcessingService: Request received for optimizing and deploying of the query " + queryId);
                            queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
                            std::string placementStrategy = queryCatalogEntry.getQueryPlacementStrategy();
                            queryPlan = queryRewritePhase->execute(queryPlan);
                            queryPlan = typeInferencePhase->execute(queryPlan);
                            queryPlacementPhase->execute(placementStrategy, queryPlan);
                            bool successful = deployAndStartQuery(queryId);
                            if (!successful) {
                                throw QueryDeploymentException("QueryRequestProcessingService: Failed to deploy query with Id " + queryId);
                            }
                            queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                        } else {
                            NES_ERROR("QueryProcessingService: Request received for query with status " << queryCatalogEntry.getQueryStatus() << " ");
                            throw InvalidQueryStatusException({QueryStatus::MarkedForStop, QueryStatus::Scheduling}, queryCatalogEntry.getQueryStatus());
                        }
                    } catch (QueryPlacementException& ex) {
                        //Rollback if failure happen while placing the query.
                        NES_ERROR(ex.what());
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (QueryDeploymentException& ex) {
                        NES_ERROR("QueryRequestProcessingService: " << ex.what());
                        //Rollback if failure happen while placing the query.
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (InvalidQueryStatusException& ex) {
                        //Rollback if failure happen while placing the query.
                        NES_ERROR("QueryRequestProcessingService: " << ex.what());
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    } catch (QueryNotFoundException& ex) {
                        //Rollback if failure happen while placing the query.
                        NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    } catch (QueryUndeploymentException& ex) {
                        //Rollback if failure happen while placing the query.
                        NES_ERROR("QueryRequestProcessingService: " << ex.what());
                    } catch (Exception& ex) {
                        NES_ERROR("QueryRequestProcessingService: " << ex.what());
                        globalExecutionPlan->removeQuerySubPlans(queryId);
                        queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                    }
                }
            }
        }
        NES_WARNING("QueryProcessingService: Terminated");
    } catch (...) {
        NES_FATAL_ERROR("QueryProcessingService: Received unexpected exception while scheduling the queries.");
        queryProcessorRunning = false;
    }
}

bool QueryRequestProcessorService::deployAndStartQuery(std::string queryId) {
    NES_DEBUG("QueryService: deploy the query");

    if (!queryCatalog->queryExists(queryId)) {
        throw QueryNotFoundException("QueryRequestProcessingService: Query with id " + queryId + " not found in the catalog.");
    }

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryRequestProcessingService: Unable to find ExecutionNodes to be deploy the query " + queryId);
        throw QueryDeploymentException("QueryRequestProcessingService: Unable to find ExecutionNodes to be deploy the query " + queryId);
    }

    bool successDeploy = deployQuery(queryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryService: deployment for query " + queryId + " successful");
    } else {
        NES_ERROR("QueryService: Failed to deploy query " + queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(queryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryService: Successfully started deployed query " + queryId);
    } else {
        NES_ERROR("QueryService: Failed to start the deployed query " + queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
    return true;
}

bool QueryRequestProcessorService::stopAndUndeployQuery(const std::string queryId) {
    NES_DEBUG("QueryService::stopAndUndeployQuery : queryId=" << queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    if (executionNodes.empty()) {
        NES_ERROR("QueryRequestProcessingService: Unable to find ExecutionNodes where the query " + queryId + " is deployed");
        return false;
    }

    NES_DEBUG("QueryService:removeQuery: stop query");
    bool successStop = stopQuery(queryId, executionNodes);
    if (successStop) {
        NES_DEBUG("QueryService:removeQuery: stop query successful");
    } else {
        NES_ERROR("QueryService:removeQuery: stop query failed");
        throw QueryUndeploymentException("Failed to stop the query " + queryId);
    }

    NES_DEBUG("QueryService:removeQuery: undeploy query");
    bool successUndeploy = undeployQuery(queryId, executionNodes);
    if (successUndeploy) {
        NES_DEBUG("QueryService:removeQuery: undeploy query successful");
    } else {
        NES_ERROR("QueryService:removeQuery: undeploy query failed");
        throw QueryUndeploymentException("Failed to undeploy the query " + queryId);
    }

    return globalExecutionPlan->removeQuerySubPlans(queryId);
}

bool QueryRequestProcessorService::deployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryService::deployQuery queryId=" << queryId);
    NES_DEBUG("QueryService: preparing for Deployment by adding port information");
    if (!queryDeployer->prepareForDeployment(queryId)) {
        NES_ERROR("QueryService: Failed to prepare for Deployment by adding port information");
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
    NES_INFO("QueryService: Finished deploying execution plan for query with Id " << queryId);
    return true;
}

bool QueryRequestProcessorService::startQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("NesCoordinator::startQuery queryId=" << queryId);

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

bool QueryRequestProcessorService::stopQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryService:stopQuery queryId=" << queryId);

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

bool QueryRequestProcessorService::undeployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("NesCoordinator::undeployQuery queryId=" << queryId);
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

bool QueryRequestProcessorService::isQueryProcessorRunning() const {
    return queryProcessorRunning;
}

bool QueryRequestProcessorService::stopQueryRequestProcessor() {
    this->queryProcessorRunning = false;
    return true;
}

}// namespace NES