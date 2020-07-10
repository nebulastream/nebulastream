#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Exceptions/QueryDeploymentException.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryPlacementPhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/QueryProcessingService.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryProcessingService::QueryProcessingService(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, QueryCatalogPtr queryCatalog,
                                               StreamCatalogPtr streamCatalog, QueryDeployerPtr queryDeployer, WorkerRPCClientPtr workerRPCClient)
    : globalExecutionPlan(globalExecutionPlan), queryCatalog(queryCatalog), queryDeployer(queryDeployer), workerRPCClient(workerRPCClient) {

    NES_INFO("QueryProcessorService()");
    typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    queryRewritePhase = QueryRewritePhase::create();
    queryPlacementPhase = QueryPlacementPhase::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
}

bool QueryProcessingService::deployQuery(std::string queryId) {
    NES_DEBUG("QueryProcessingService::deployQuery queryId=" << queryId);

    NES_DEBUG("QueryProcessingService: preparing for Deployment by adding port information");
    if (!queryDeployer->prepareForDeployment(queryId)) {
        NES_ERROR("QueryProcessingService: Failed to prepare for Deployment by adding port information");
        return false;
    }
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    if (executionNodes.empty()) {
        NES_ERROR("QueryProcessingService: Unable to find execution plan for the query with id " << queryId);
        return false;
    }

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryProcessingService::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
        if (!querySubPlan) {
            NES_WARNING("QueryProcessingService : unable to find query sub plan with id " << queryId);
            return false;
        }
        //FIXME: we are considering only one root operator
        OperatorNodePtr rootOperator = querySubPlan->getRootOperators()[0];

        std::string nesNodeIp = executionNode->getNesNode()->getIp();

        NES_DEBUG("QueryProcessingService:deployQuery: " << queryId << " to " << nesNodeIp);
        bool success = workerRPCClient->registerQuery(nesNodeIp, queryId, rootOperator);
        if (success) {
            NES_DEBUG("QueryProcessingService:deployQuery: " << queryId << " to " << nesNodeIp << " successful");
        } else {
            NES_ERROR("QueryProcessingService:deployQuery: " << queryId << " to " << nesNodeIp << "  failed");
            return false;
        }
    }
    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
    NES_INFO("QueryProcessingService: Finished deploying execution plan for query with Id " << queryId);
    return true;
}

int QueryProcessingService::operator()() {

    while (true) {
        const std::vector<QueryCatalogEntryPtr> queryCatalogEntryBatch = queryCatalog->getQueriesToSchedule();
        if (queryCatalogEntryBatch.empty()) {
            //process the queries using a-query-at-a-time model
            for (auto queryCatalogEntry : queryCatalogEntryBatch) {

                std::string placementStrategy = queryCatalogEntry->getQueryPlacementStrategy();
                auto queryPlan = queryCatalogEntry->getQueryPlan();
                std::string queryId = queryPlan->getQueryId();
                try {
                    queryCatalog->markQueryAs(queryId, QueryStatus::Scheduling);
                    queryPlan = queryRewritePhase->execute(queryPlan);
                    queryPlan = typeInferencePhase->execute(queryPlan);
                    queryPlacementPhase->execute(placementStrategy, queryPlan);
                    if (!deployQuery(queryId)) {
                        throw QueryDeploymentException("Failed to deploy query with Id " + queryId);
                    }
                    queryCatalog->markQueryAs(queryId, QueryStatus::Running);
                } catch (QueryPlacementException ex) {
                    //Rollback if failure happen while placing the query.
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (QueryDeploymentException ex) {
                    //Rollback if failure happen while placing the query.
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                } catch (Exception ex) {
                    queryCatalog->markQueryAs(queryId, QueryStatus::Failed);
                }
            }
        }
    }
}

}// namespace NES