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
#include <nlohmann/json_fwd.hpp>
#include <vector>

namespace NES {

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

using ExecutionNodeWLock = std::shared_ptr<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>;
using TopologyNodeWLock = std::shared_ptr<folly::Synchronized<TopologyNodePtr>::WLockedPtr>;

/**
 * This class holds the global execution plan for the NES system. The nodes in this graph are represented by ExecutionNode class.
 */
class GlobalExecutionPlan {

  public:
    static GlobalExecutionPlanPtr create();

    /**
     * @brief Add a decomposed query plan belonging to a shared query plan on the given execution node.
     * @note If the execution node does not exists then create one and create parent child relationships based on the
     * topology node.
     * @param lockedTopologyNode: the locked topology node that will be represented by the execution node
     * @param decomposedQueryPlan: the decomposed query plan
     * @return true if success else false
     */
    bool addDecomposedQueryPlan(const TopologyNodeWLock& lockedTopologyNode, DecomposedQueryPlanPtr decomposedQueryPlan);

    /**
     * @brief Update the decomposed query plan state to the new query state.
     * Note: the operation is successful only if the given decomposed plan version matches the stored decomposed query
     * plan version.
     * @param executionNodeId: the id of the execution node
     * @param sharedQueryId: the shared query id
     * @param decomposedQueryPlanId: the decomposed query id
     * @param expectedVersion: the expected version
     * @param newDecomposedQueryPlanState: the new state
     * @return true if successful else false
     */
    bool updateDecomposedQueryPlanState(ExecutionNodeId executionNodeId,
                                        SharedQueryId sharedQueryId,
                                        DecomposedQueryPlanId decomposedQueryPlanId,
                                        DecomposedQueryPlanVersion expectedVersion,
                                        QueryState newDecomposedQueryPlanState);

    /**
     * @brief Get the identifier of the shared query plans hosted on the given execution node
     * @param executionNodeId: the execution node id
     * @return vector of shared query ids
     */
    std::set<SharedQueryId> getPlacedSharedQueryIds(ExecutionNodeId executionNodeId) const;

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
     * Add execution node as root of the execution graph
     * @param executionNodeId : the id of the execution node
     * @return true if operation succeeds
     */
    bool addExecutionNodeAsRoot(ExecutionNodeId executionNodeId);

    /**
     * Remove all the decomposed query plans for the input shared query plan id
     * @param sharedQueryId : the shared query id used for removing the input query
     * @return true if successful else false
     */
    bool removeAllDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * @brief Remove the decomposed query plan
     * @param executionNodeId: the execution node id
     * @param sharedQueryId: the shared query id
     * @param decomposedQueryPlanId: the decomposed query plan id
     * @param decomposedQueryPlanVersion: the decomposed query plan version
     * @return
     */
    bool removeDecomposedQueryPlan(ExecutionNodeId executionNodeId,
                                   SharedQueryId sharedQueryId,
                                   DecomposedQueryPlanId decomposedQueryPlanId,
                                   DecomposedQueryPlanVersion decomposedQueryPlanVersion);

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

    /**
     * @brief get the json representation of execution plan of a query
     * @param sharedQueryId of the shared query plan for which the execution plan is needed
     * @return a JSON object representing the execution plan
     */
    nlohmann::json getAsJson(SharedQueryId sharedQueryId);

  private:
    /**
     * Remove the execution node from the graph
     * @param executionNodeId: id of the execution node to be removed
     * @return true if operation succeeds
     */
    bool removeExecutionNode(ExecutionNodeId executionNodeId);

    /*    *//**
     * Map the input execution node with different sub query plans it has
     * @param executionNode : the input execution node
     *//*
    void mapExecutionNodeToSharedQueryId(const ExecutionNodePtr& executionNode);*/

    /**
     * Index based on nodeId for faster access to the execution nodes
     */
    folly::Synchronized<std::map<ExecutionNodeId, folly::Synchronized<ExecutionNodePtr>>> idToExecutionNodeMap;

    /**
     * Index based on shared query Id for faster access to the execution nodes
     */
    folly::Synchronized<std::map<SharedQueryId, std::set<ExecutionNodeId>>> sharedQueryIdToExecutionNodeIdMap;

    /**
     * List of root node ids
     */
    folly::Synchronized<std::vector<ExecutionNodeId>> rootExecutionNodeIds;
};

}// namespace Optimizer
}// namespace NES

#endif// NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBALEXECUTIONPLAN_HPP_
