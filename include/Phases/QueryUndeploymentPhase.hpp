#ifndef NES_QUERYUNDEPLOYMENTPHASE_HPP
#define NES_QUERYUNDEPLOYMENTPHASE_HPP

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

class QueryUndeploymentPhase;
typedef std::shared_ptr<QueryUndeploymentPhase> QueryUndeploymentPhasePtr;

/**
 * @brief This class is responsible for undeploying and stopping a running query
 */
class QueryUndeploymentPhase {

  public:
    /**
     * @brief Returns a smart pointer to the QueryUndeploymentPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryUndeploymentPhase
     */
    static QueryUndeploymentPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method for stopping and undeploying the query with the given id
     * @param queryId : id of the query
     * @return true if successful
     */
    bool execute(const std::string queryId);

  private:
    explicit QueryUndeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method remove query from nodes
     * @param queryId
     * @return bool indicating success
     */
    bool undeployQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    /**
     * @brief method to stop a query
     * @param queryId
     * @return bool indicating success
     */
    bool stopQuery(std::string queryId, std::vector<ExecutionNodePtr> executionNodes);

    WorkerRPCClientPtr workerRPCClient;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES

#endif//NES_QUERYUNDEPLOYMENTPHASE_HPP
