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

#ifndef NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_
#define NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_

#include <Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <list>
#include <map>
#include <memory>
#include <set>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

using PlacedQuerySubPlans = std::map<SharedQueryId, std::map<QuerySubPlanId, QueryPlanPtr>>;

/**
 * This class contains information about the physical node, a map of query sub plans that need to be executed
 * on the physical node, and some additional configurations.
 */
class ExecutionNode : public Node {

  public:
    static ExecutionNodePtr createExecutionNode(TopologyNodePtr physicalNode);

    virtual ~ExecutionNode() = default;

    /**
     * Check if a query sub plan with given Id exists or not
     * @param sharedQueryId : Id of the sub plan
     * @return true if the plan exists else false
     */
    bool hasQuerySubPlans(SharedQueryId sharedQueryId);

    /**
     * Get execution node id
     * @return id of the execution node
     */
    ExecutionNodeId getId() const;

    /**
     * Get the nes node for the execution node.
     * @return the nes node
     */
    TopologyNodePtr getTopologyNode();

    /**
     * Create a new entry for query sub plan
     * @param sharedQueryId : the query ID
     * @param querySubPlan : the query sub plan
     * @return true if operation is successful
     */
    bool addNewQuerySubPlan(SharedQueryId sharedQueryId, const QueryPlanPtr& querySubPlan);

    /**
     * Update an existing query sub plan
     * @param sharedQueryId : query id
     * @param querySubPlans : the new query sub plan
     * @return true if successful
     */
    bool updateQuerySubPlans(SharedQueryId sharedQueryId, std::vector<QueryPlanPtr> querySubPlans);

    /**
     * Get Query query sub plan for the given Id
     * @param sharedQueryId
     * @return Query sub plan
     */
    std::vector<QueryPlanPtr> getQuerySubPlans(SharedQueryId sharedQueryId);

    /**
     * Remove existing query sub plans belonging to a shared query
     * @param sharedQueryId
     * @return true if operation succeeds
     */
    bool removeQuerySubPlans(SharedQueryId sharedQueryId);

    /**
     * Remove a single existing query sub plan
     * @param sharedQueryId the id of the shared query to which the query sub plan belongs
     * @param querySubPlanId the id of the query sub plan to remove
     * @return true if operation succeeds
     */
    bool removeQuerySubPlan(SharedQueryId sharedQueryId, QuerySubPlanId querySubPlanId);

    /**
     * Get the map of all query sub plans
     * @return
     */
    PlacedQuerySubPlans getAllQuerySubPlans();

    /**
     * Get the resources occupied by the query sub plans for the input query id.
     * @param sharedQueryId : the input shared query plan id
     */
    uint32_t getOccupiedResources(SharedQueryId sharedQueryId);

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;

    std::vector<std::string> toMultilineString() override;

    /**
     * @brief Get identifier of all shared query plans placed on the execution node
     * @return set of shared query plan ids
     */
    std::set<SharedQueryId> getPlacedSharedQueryPlanIds();

    /**
     * @brief Get the amount of resources used by a specific sub plan
     * @param querySubPlan The query sub plan
     * @return the amount of resources
     */
    static uint32_t getOccupiedResourcesForSubPlan(const QueryPlanPtr& querySubPlan) ;

    /**
     * @brief returns a pointer to a specific query sub plan
     * @param sharedQueryId the id of the shared query to which the sub plan belongs
     * @param subPlanId the id of the sub plan itself
     * @return A pointer to the sub plan
     */
    QueryPlanPtr getQuerySubPlan(SharedQueryId sharedQueryId, QuerySubPlanId subPlanId);

  private:
    explicit ExecutionNode(const TopologyNodePtr& physicalNode);

    /**
     * Execution node id.
     * Same as physical node id.
     */
    const ExecutionNodeId id;

    /**
     * Physical Node information
     */
    const TopologyNodePtr topologyNode;

    /**
     * a map of placed Query Sub Plans
     */
    PlacedQuerySubPlans mapOfSharedQueryToQuerySubPlans;
};
}// namespace NES

#endif// NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_
