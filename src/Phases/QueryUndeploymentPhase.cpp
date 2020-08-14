#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryUndeploymentPhase::QueryUndeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient)
    : globalExecutionPlan(globalExecutionPlan), workerRPCClient(workerRpcClient) {}

QueryUndeploymentPhasePtr QueryUndeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryUndeploymentPhase>(QueryUndeploymentPhase(globalExecutionPlan, workerRpcClient));
}

bool QueryUndeploymentPhase::execute(const uint64_t queryId) {
    NES_DEBUG("QueryService::stopAndUndeployQuery : queryId=" << queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    if (executionNodes.empty()) {
        NES_ERROR("QueryRequestProcessingService: Unable to find ExecutionNodes where the query " + std::to_string(queryId) + " is deployed");
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

bool QueryUndeploymentPhase::stopQuery(uint64_t queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryService:stopQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        const auto& nesNode = executionNode->getNesNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryService::stopQuery at execution node with id=" << executionNode->getId() << " and IP=" << rpcAddress);
        bool success = workerRPCClient->stopQuery(rpcAddress, queryId);
        if (success) {
            NES_DEBUG("QueryService::stopQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryService::stopQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
    }
    return true;
}

bool QueryUndeploymentPhase::undeployQuery(uint64_t queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("NesCoordinator::undeployQuery queryId=" << queryId);
    for (ExecutionNodePtr executionNode : executionNodes) {
        const auto& nesNode = executionNode->getNesNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("NESCoordinator::undeployQuery query at execution node with id=" << executionNode->getId() << " and IP=" << rpcAddress);
        bool success = workerRPCClient->unregisterQuery(rpcAddress, queryId);
        if (success) {
            NES_DEBUG("NESCoordinator::undeployQuery  query " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("NESCoordinator::undeployQuery  " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
    }
    return true;
}
}// namespace NES