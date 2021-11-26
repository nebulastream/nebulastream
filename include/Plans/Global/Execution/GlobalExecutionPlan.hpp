/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBAL_EXECUTION_PLAN_HPP_
#define NES_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBAL_EXECUTION_PLAN_HPP_

#include <Plans/Query/QueryId.hpp>
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
     * Add execution node as parent of another execution node. If the node already exists then simply update the node.
     * @param childId: id of the child node
     * @param parentExecutionNode: the parent execution node
     * @return true if operation succeeds
     */
    bool addExecutionNodeAsParentTo(uint64_t childId, const ExecutionNodePtr& parentExecutionNode);

    /**
     * Add execution node without any connectivity
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
    bool removeExecutionNode(uint64_t id);

    /**
     * Remove all the query sub plans for the input query
     * @param queryId : the query id used for removing the input query
     * @return true if successful else false
     */
    bool removeQuerySubPlans(QueryId queryId);

    /**
     * Find is execution node exists.
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    bool checkIfExecutionNodeExists(uint64_t id);

    /**
     * Check if the executionNode is a root node.
     * @param id: id of the execution node.
     * @return true if execution node with given id is a root node.
     */
    bool checkIfExecutionNodeIsARoot(uint64_t id);

    /**
     * Get the execution node
     * @param id: id of the execution node
     * @return true if operation succeeds
     */
    ExecutionNodePtr getExecutionNodeByNodeId(uint64_t id);

    /**
     * removes a ExecutionNode from the QueryIdIndex if executionNode no longer has any QSP for
     * Query with QueryId
     */
    void removeExecutionNodeFromQueryIdIndex(QueryId queryId, uint64_t nodeId);

    /**
     * Get the nodes to be scheduled/deployed
     * @return vector of execution nodes to be scheduled
     */
    std::vector<ExecutionNodePtr> getExecutionNodesToSchedule();

    /**
     * Get root nodes
     * @return vector containing all root nodes
     */
    std::vector<ExecutionNodePtr> getRootNodes();

    /**
     * Return list of Execution Serialization used for placing operators of the input query Id
     * @param queryId : Id of the query
     * @return vector containing execution nodes
     */
    std::vector<ExecutionNodePtr> getExecutionNodesByQueryId(QueryId queryId);

    /**
     * @brief Get all execution nodes in the execution plan
     * @return list of execution nodes
     */
    std::vector<ExecutionNodePtr> getAllExecutionNodes();

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
     * @param queryId : the id of the query
     * @return a map of topology node id to resources occupied
     */
    std::map<uint64_t, uint32_t> getMapOfTopologyNodeIdToOccupiedResource(QueryId queryId);

  private:
    explicit GlobalExecutionPlan();
    /**
     * Map the input execution node with different sub query plans it has
     * @param executionNode : the input execution node
     */
    void mapExecutionNodeToQueryId(const ExecutionNodePtr& executionNode);

    /**
     * Index based on nodeId for faster access to the execution nodes
     */
    std::map<uint64_t, ExecutionNodePtr> nodeIdIndex;

    /**
     * Index based on query Id for faster access to the execution nodes
     */
    std::map<QueryId, std::vector<ExecutionNodePtr>> queryIdIndex;

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

#endif// NES_INCLUDE_PLANS_GLOBAL_EXECUTION_GLOBAL_EXECUTION_PLAN_HPP_
