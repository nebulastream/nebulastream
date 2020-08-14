#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient)
    : globalExecutionPlan(globalExecutionPlan), workerRPCClient(workerRpcClient) {}

QueryDeploymentPhasePtr QueryDeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryDeploymentPhase>(QueryDeploymentPhase(globalExecutionPlan, workerRpcClient));
}

bool QueryDeploymentPhase::execute(uint64_t queryId) {

    NES_DEBUG("QueryService: deploy the query");

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryRequestProcessingService: Unable to find ExecutionNodes to be deploy the query " << queryId);
        throw QueryDeploymentException("QueryRequestProcessingService: Unable to find ExecutionNodes to be deploy the query " + queryId);
    }

    bool successDeploy = deployQuery(queryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryService: deployment for query " + std::to_string(queryId) + " successful");
    } else {
        NES_ERROR("QueryService: Failed to deploy query " << queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(queryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryService: Successfully started deployed query " << queryId);
    } else {
        NES_ERROR("QueryService: Failed to start the deployed query " << queryId);
        throw QueryDeploymentException("QueryService: Failed to deploy query " + queryId);
    }
    return true;
}

bool QueryDeploymentPhase::deployQuery(uint64_t queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryService::deployQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryService::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
        if (!querySubPlan) {
            NES_WARNING("QueryService : unable to find query sub plan with id " << queryId);
            return false;
        }
        //FIXME: we are considering only one root operator
        OperatorNodePtr rootOperator = querySubPlan->getRootOperators()[0];
        const auto& nesNode = executionNode->getNesNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryService:deployQuery: " << queryId << " to " << rpcAddress);
        bool success = workerRPCClient->registerQuery(rpcAddress, queryId, rootOperator);
        if (success) {
            NES_DEBUG("QueryService:deployQuery: " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryService:deployQuery: " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
    }
    NES_INFO("QueryService: Finished deploying execution plan for query with Id " << queryId);
    return true;
}

bool QueryDeploymentPhase::startQuery(uint64_t queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("NesCoordinator::startQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        const auto& nesNode = executionNode->getNesNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("NesCoordinator::startQuery at execution node with id=" << executionNode->getId() << " and IP=" << ipAddress);
        bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        if (success) {
            NES_DEBUG("NesCoordinator::startQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("NesCoordinator::startQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
    }
    return true;
}

}// namespace NES