#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryDeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryDeploymentPhase::QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient)
    : globalExecutionPlan(globalExecutionPlan), workerRPCClient(workerRpcClient) {
    NES_DEBUG("QueryDeploymentPhase()");
}

QueryDeploymentPhase::~QueryDeploymentPhase() {
    NES_DEBUG("~QueryDeploymentPhase()");
}
QueryDeploymentPhasePtr QueryDeploymentPhase::create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryDeploymentPhase>(QueryDeploymentPhase(globalExecutionPlan, workerRpcClient));
}

bool QueryDeploymentPhase::execute(QueryId queryId) {

    NES_DEBUG("QueryDeploymentPhase: deploy the query");

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the query " << queryId);
        throw QueryDeploymentException("QueryDeploymentPhase: Unable to find ExecutionNodes to be deploy the query " + queryId);
    }

    bool successDeploy = deployQuery(queryId, executionNodes);
    if (successDeploy) {
        NES_DEBUG("QueryDeploymentPhase: deployment for query " + std::to_string(queryId) + " successful");
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to deploy query " << queryId);
        throw QueryDeploymentException("QueryDeploymentPhase: Failed to deploy query " + queryId);
    }

    NES_DEBUG("QueryService: start query");
    bool successStart = startQuery(queryId, executionNodes);
    if (successStart) {
        NES_DEBUG("QueryDeploymentPhase: Successfully started deployed query " << queryId);
    } else {
        NES_ERROR("QueryDeploymentPhase: Failed to start the deployed query " << queryId);
        throw QueryDeploymentException("QueryDeploymentPhase: Failed to deploy query " + queryId);
    }
    return true;
}

bool QueryDeploymentPhase::deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {

    NES_DEBUG("QueryDeploymentPhase::deployQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryDeploymentPhase::registerQueryInNodeEngine serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (!querySubPlans.empty()) {
            NES_WARNING("QueryDeploymentPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        const auto& nesNode = executionNode->getPhysicalNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress);

        for(auto& querySubPlan: querySubPlans){
            //FIXME: we are considering only one root operator
            OperatorNodePtr rootOperator = querySubPlan->getRootOperators()[0];
            bool success = workerRPCClient->registerQuery(rpcAddress, queryId, rootOperator);
            if (success) {
                NES_DEBUG("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryDeploymentPhase:deployQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
    }
    NES_INFO("QueryDeploymentPhase: Finished deploying execution plan for query with Id " << queryId);
    return true;
}

bool QueryDeploymentPhase::startQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("QueryDeploymentPhase::startQuery queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        const auto& nesNode = executionNode->getPhysicalNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryDeploymentPhase::startQuery at execution node with id=" << executionNode->getId() << " and IP=" << ipAddress);
        bool success = workerRPCClient->startQuery(rpcAddress, queryId);
        if (success) {
            NES_DEBUG("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryDeploymentPhase::startQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
    }
    return true;
}

}// namespace NES