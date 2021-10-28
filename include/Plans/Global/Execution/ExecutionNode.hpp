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

#ifndef NES_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTION_NODE_HPP_
#define NES_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTION_NODE_HPP_

#include <Nodes/Node.hpp>
#include <Plans/Query/QueryId.hpp>
#include <list>
#include <map>
#include <memory>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

/**
 * This class contains information about the physical node, a map of query sub plans that need to be executed
 * on the physical node, and some additional configurations.
 */
class ExecutionNode : public Node {

  public:
    static ExecutionNodePtr createExecutionNode(TopologyNodePtr physicalNode, QueryId queryId, OperatorNodePtr operatorNode);
    static ExecutionNodePtr createExecutionNode(TopologyNodePtr physicalNode);

    virtual ~ExecutionNode() = default;

    /**
     * Check if a query sub plan with given Id exists or not
     * @param queryId : Id of the sub plan
     * @return true if the plan exists else false
     */
    bool hasQuerySubPlans(QueryId queryId);

    /**
     * Get execution node id
     * @return id of the execution node
     */
    uint64_t getId() const;

    /**
     * Get the nes node for the execution node.
     * @return the nes node
     */
    TopologyNodePtr getTopologyNode();

    /**
     * Create a new entry for query sub plan
     * @param queryId : the query ID
     * @param querySubPlan : the query sub plan
     * @return true if operation is successful
     */
    bool addNewQuerySubPlan(QueryId queryId, const QueryPlanPtr& querySubPlan);

    /**
     * Update an existing query sub plan
     * @param queryId : query id
     * @param querySubPlans : the new query sub plan
     * @return true if successful
     */
    bool updateQuerySubPlans(QueryId queryId, std::vector<QueryPlanPtr> querySubPlans);

    /**
     * Get Query subPlan for the given Id
     * @param queryId
     * @return Query sub plan
     */
    std::vector<QueryPlanPtr> getQuerySubPlans(QueryId queryId);

    /**
     * Remove existing subPlan
     * @param queryId
     * @return true if operation succeeds
     */
    bool removeQuerySubPlans(QueryId queryId);

    /**
     * Get the map of all query sub plans
     * @return
     */
    std::map<QueryId, std::vector<QueryPlanPtr>> getAllQuerySubPlans();

    /**
     * Get the resources occupied by the query sub plans for the input query id.
     * @param queryId : the input query id
     */
    uint32_t getOccupiedResources(QueryId queryId);

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;

    std::vector<std::string> toMultilineString() override;

  private:
    explicit ExecutionNode(const TopologyNodePtr& physicalNode, QueryId queryId, OperatorNodePtr operatorNode);

    explicit ExecutionNode(const TopologyNodePtr& physicalNode);

    /**
     * Execution node id.
     * Same as physical node id.
     */
    const uint64_t id;

    /**
     * Physical Node information
     */
    const TopologyNodePtr topologyNode;

    /**
     * map of queryPlans
     */
    std::map<QueryId, std::vector<QueryPlanPtr>> mapOfQuerySubPlans;
    const std::vector<std::string> toMultilineString() const;
};
}// namespace NES

#endif// NES_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTION_NODE_HPP_
