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
#include <map>
#include <memory>
#include <vector>

namespace NES {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

/**
 * This class holds the global execution plan for the NES system. The nodes in this graph are represented by ExecutionNode class.
 */
class GlobalExecutionPlan {

  public:
    static GlobalExecutionPlanPtr create();

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
    //TODO: what should we do about its children? Also, a good location to release the occupied resources.
    bool removeExecutionNode(ExecutionNodeId id);

    /**
     * Remove all the query sub plans for the input query
     * @param sharedQueryId : the shared query id used for removing the input query
     * @return true if successful else false
     */
    bool removeQuerySubPlans(SharedQueryId sharedQueryId);

    /**
     * Find is execution node exists.
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    bool checkIfExecutionNodeExists(ExecutionNodeId id);

    /**
     * Get the execution node
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    ExecutionNodePtr getExecutionNodeById(ExecutionNodeId id);

    /**
     * Return list of Execution Serialization used for placing operators of the input query Id
     * @param sharedQueryId : Id of the shared query
     * @return vector containing execution nodes
     */
    std::vector<ExecutionNodePtr> getExecutionNodesByQueryId(SharedQueryId sharedQueryId);

    /**
     * Get the execution plan as string representation
     * @return returns string representation of the plan
     */
    std::string getAsString();

    /**
     * Add execution node to the collection of execution nodes to schedule
     * @param executionNode : execution node to schedule
     */
    void scheduleExecutionNode(const ExecutionNodePtr& executionNode);

    /**
     * @brief Get the map of topology node id to the amount of resources occupied by the query
     * @param sharedQueryId : the id of the query
     * @return a map of topology node id to resources occupied
     */
    std::map<uint64_t, uint32_t> getMapOfWorkerIdToOccupiedResource(QueryId sharedQueryId);

    /**
     * @brief removes a query subplan if it exists at a specific node
     * @param executionNodeId the id of the node hosting the subplans
     * @param sharedQueryId the id of the shared query to which the subplan belongs
     * @param subPlanId the id of the subplan
     * @return true if the subplan was found and removed, false if it could not be found
     */
    bool removeQuerySubPlanFromNode(ExecutionNodeId executionNodeId, SharedQueryId sharedQueryId, DecomposedQueryPlanId subPlanId);

  private:
    /**
     * Map the input execution node with different sub query plans it has
     * @param executionNode : the input execution node
     */
    void mapExecutionNodeToSharedQueryId(const ExecutionNodePtr& executionNode);

    /**
     * Index based on nodeId for faster access to the execution nodes
     */
    std::map<ExecutionNodeId, ExecutionNodePtr> executionNodeIdToExecutionNodeMap;

    /**
     * Index based on shared query Id for faster access to the execution nodes
     */
    std::map<SharedQueryId, std::vector<ExecutionNodePtr>> sharedQueryIdToExecutionNodeMap;

    /**
     * List Of ExecutionNodes to be deployed. This list contains all nodes with the scheduled flag as false.
     */
    std::vector<ExecutionNodePtr> executionNodesToSchedule;

    /**
     * List of root nodes
     */
    std::vector<ExecutionNodePtr> rootNodes;
};

}// namespace NES

#endif // NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBALEXECUTIONPLAN_HPP_
