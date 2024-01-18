/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBALEXECUTIONPLAN_HPP_
#define NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBALEXECUTIONPLAN_HPP_

#include <Identifiers.hpp>
#include <Util/QueryState.hpp>
#include <folly/Synchronized.h>
#include <map>
#include <memory>
#include <vector>

namespace NES::Optimizer {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

using ExecutionNodeWLock = std::shared_ptr<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>;

/**
 * This class holds the global execution plan for the NES system. The nodes in this graph are represented by ExecutionNode class.
 */
class GlobalExecutionPlan {

  public:
    static GlobalExecutionPlanPtr create();

    /**
     * @brief Add a decomposed query plan originated from a shared query plan on an execution node representing the
     * given topology node
     * @note If the execution node does not exists then create one and create parent child relationships based on the
     * topology node.
     * @param topologyNode: the topology node that will be represented by the execution node
     * @param decomposedQueryPlan: the decomposed query plan
     * @return true if success else false
     */
    bool addDecomposedQueryPlan(TopologyNodePtr topologyNode, DecomposedQueryPlanPtr decomposedQueryPlan);

    /**
     * @brief Update the decomposed query plan state to the new query state.
     * Note: the operation is successful only if the given decomposed plan version matches the stored decomposed query
     * plan version.
     * @param executionNodeId: the id of the execution node
     * @param sharedQueryId
     * @param expectedVersion
     * @param newDecomposedQueryPlanState
     * @return
     */
    bool updateDecomposedQueryPlanState(ExecutionNodeId executionNodeId,
                                        SharedQueryId sharedQueryId,
                                        DecomposedQueryPlanVersion expectedVersion,
                                        QueryState newDecomposedQueryPlanState);

    /**
     * @brief Get the identifier of the shared query plans hosted on the given execution node
     * @param executionNodeId: the execution node id
     * @return vector of shared query ids
     */
    std::vector<SharedQueryId> getHostedSharedQueryIds(ExecutionNodeId executionNodeId);

    /**
     * @brief Get the copy of the decomposed query plan with the given id
     * @param executionNodeId : the execution node hosting the shared query id
     * @param sharedQueryId : the shared query id
     * @param decomposedQueryPlanId : the decomposed query id
     * @return copy of the decomposed query plan
     */
    DecomposedQueryPlanPtr getCopyOfDecomposedQueryPlan(ExecutionNodeId executionNodeId,
                                                        SharedQueryId sharedQueryId,
                                                        DecomposedQueryPlanId decomposedQueryPlanId);

    /**
     * @brief Get the copy of all decomposed query plans originating from the given shared query id and hosted on the
     * execution node with given id.
     * @param executionNodeId : the id of the execution node
     * @param sharedQueryId : the id of the shared query
     * @return the vector containing copies of decomposed query plans
     */
    std::vector<DecomposedQueryPlanPtr> getCopyOfAllDecomposedQueryPlans(ExecutionNodeId executionNodeId,
                                                                         SharedQueryId sharedQueryId);

    /**
     * @brief Add execution node and automatically add parent child relation ship with existing execution nodes.
     * @param executionNode: the execution node to add
     * @return true if added else false
     */
    bool addExecutionNode(const ExecutionNodePtr& executionNode);

    /**
     * Add execution node as root of the execution graph
     * @param executionNode : Node to be added
     * @return true if operation succeeds
     */
    bool addExecutionNodeAsRoot(const ExecutionNodePtr& executionNode);

    /**
     * Remove the execution node from the graph
     * @param id: id of the execution node to be removed
     * @return true if operation succeeds
     */
    bool removeExecutionNode(ExecutionNodeId id);

    /**
     * Remove all the decomposed query plans for the input shared query plan id
     * @param sharedQueryId : the shared query id used for removing the input query
     * @return true if successful else false
     */
    bool removeAllDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * Get the execution node
     * @param executionNodeId: id of the execution node
     * @return true if operation succeeds
     */
    ExecutionNodeWLock getLockedExecutionNode(ExecutionNodeId executionNodeId);

    /**
     * Return list of Execution Serialization used for placing operators of the input query Id
     * @param sharedQueryId : Id of the shared query
     * @return vector containing execution nodes
     */
    std::vector<ExecutionNodeWLock> getLockedExecutionNodesHostingSharedQueryId(SharedQueryId sharedQueryId);

    /**
     * Get the execution plan as string representation
     * @return returns string representation of the plan
     */
    std::string getAsString();

    //    /**
    //     * Add execution node to the collection of execution nodes to schedule
    //     * @param executionNode : execution node to schedule
    //     */
    //    void scheduleExecutionNode(const ExecutionNodePtr& executionNode);

    //    /**
    //     * @brief Get the map of topology node id to the amount of resources occupied by the query
    //     * @param sharedQueryId : the id of the query
    //     * @return a map of topology node id to resources occupied
    //     */
    //    std::map<uint64_t, uint32_t> getMapOfWorkerIdToOccupiedResource(SharedQueryId sharedQueryId);

    /**
     * @brief removes a decomposed query plan if it exists at a specific node
     * @param executionNodeId the id of the node hosting the decomposed query plan
     * @param sharedQueryId the id of the shared query
     * @param decomposedQueryPlanId the id of the decomposed query plan
     * @return true if the decomposed was found and removed, false if it could not be found
     */
    bool removeQuerySubPlanFromNode(ExecutionNodeId executionNodeId,
                                    SharedQueryId sharedQueryId,
                                    DecomposedQueryPlanId decomposedQueryPlanId);

  private:
    /**
     * Map the input execution node with different sub query plans it has
     * @param executionNode : the input execution node
     */
    void mapExecutionNodeToSharedQueryId(const ExecutionNodePtr& executionNode);

    /**
     * Index based on nodeId for faster access to the execution nodes
     */
    folly::Synchronized<std::map<ExecutionNodeId, folly::Synchronized<ExecutionNodePtr>>> idToExecutionNodeMap;

    /**
     * Index based on shared query Id for faster access to the execution nodes
     */
    folly::Synchronized<std::map<SharedQueryId, folly::Synchronized<std::vector<ExecutionNodeId>>>>
        sharedQueryIdToExecutionNodeIdMap;

    /**
     * List of root node ids
     */
    std::vector<ExecutionNodeId> rootExecutionNodeIds;
};

}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBALEXECUTIONPLAN_HPP_
