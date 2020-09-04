#ifndef NES_QUERYDEPLOYMENTPHASE_HPP
#define NES_QUERYDEPLOYMENTPHASE_HPP

#include <Plans/Query/QueryId.hpp>
#include <iostream>
#include <memory>
#include <vector>

namespace NES {

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryDeploymentPhase;
typedef std::shared_ptr<QueryDeploymentPhase> QueryDeploymentPhasePtr;

/**
 * @brief The query deployment phase is responsible for deploying the query plan for a query to respective worker nodes.
 */
class QueryDeploymentPhase {
  public:
    /**
     * @brief Returns a smart pointer to the QueryDeploymentPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryDeploymentPhase
     */
    static QueryDeploymentPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method for deploying and starting the query
     * @param queryId : the query Id of the query to be deployed and started
     * @return true if successful else false
     */
    bool execute(QueryId queryId);
    ~QueryDeploymentPhase();

  private:
    explicit QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);
    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);

    WorkerRPCClientPtr workerRPCClient;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES
#endif//NES_QUERYDEPLOYMENTPHASE_HPP
