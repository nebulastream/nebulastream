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

bool QueryUndeploymentPhase::execute(const std::string queryId) {
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

bool QueryUndeploymentPhase::stopQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryService:stopQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        std::string nesNodeIp = executionNode->getNesNode()->getIp();
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

bool QueryUndeploymentPhase::undeployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("NesCoordinator::undeployQuery queryId=" << queryId);
    for (ExecutionNodePtr executionNode : executionNodes) {
        std::string nesNodeIp = executionNode->getNesNode()->getIp();
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