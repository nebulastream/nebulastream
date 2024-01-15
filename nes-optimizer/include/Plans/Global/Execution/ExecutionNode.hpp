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

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Optimizer {

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

using PlacedDecomposedQueryPlans = std::map<SharedQueryId, std::map<DecomposedQueryPlanId, DecomposedQueryPlanPtr>>;

/**
 * This class contains information about the physical node, a map of decomposed query plans that need to be executed
 * on the physical node, and some additional configurations.
 */
class ExecutionNode : public Node {

  public:
    static ExecutionNodePtr createExecutionNode(TopologyNodePtr physicalNode);

    virtual ~ExecutionNode() = default;

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
     * Register a new decomposed query plan for the given shared query id
     * @param sharedQueryId : the shared query id
     * @param decomposedQueryPlan : the decomposed query plan
     * @return true if operation is successful
     */
    bool registerNewDecomposedQueryPlan(SharedQueryId sharedQueryId, const DecomposedQueryPlanPtr& decomposedQueryPlan);

    /**
     * Update decomposed query plans for the given shared query plan id
     * @note: We store a copy of the supplied decomposed query plans
     * @param sharedQueryId : the shared query id
     * @param decomposedQueryPlans : the decomposed query plans
     */
    void updateDecomposedQueryPlans(SharedQueryId sharedQueryId, std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans);

    /**
     * @brief Get decomposed query plans belonging to the given shared query Id
     * @param sharedQueryId: the shared query id
     * @return vector containing copies of placed decomposed query plans
     */
    std::vector<DecomposedQueryPlanPtr> getAllDecomposedQueryPlans(SharedQueryId sharedQueryId) const;

    /**
      * @brief Get the decomposed query plan belonging to a given shared query id and has the provided decomposed query plan id
      * @param sharedQueryId: shared query id
      * @param decomposedQueryPlanId: decomposed query plan id
      * @return the copy of the decomposed query plan
      */
    DecomposedQueryPlanPtr getCopyOfDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                                        DecomposedQueryPlanId decomposedQueryPlanId) const;

    /**
     * Remove existing decomposed query plans belonging to a shared query plan
     * @param sharedQueryId: the shared query plans
     * @return true if operation succeeds
     */
    bool removeDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * Check if there are registered decomposed query plans hosted on the execution node
     * @param sharedQueryId: the shared query plans
     * @return true if operation succeeds
     */
    bool hasRegisteredDecomposedQueryPlans(SharedQueryId sharedQueryId);

    /**
     * Remove a decomposed query plan belonging to a shared query plan with given decomposed query plan id
     * @param sharedQueryId the id of the shared query
     * @param decomposedQueryPlanId the id of the decomposed query plan to remove
     * @return true if operation succeeds
     */
    bool removeDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryPlanId decomposedQueryPlanId);

    /**
     * Get the map of all decomposed query plans
     * @return map of shared query plan id to decomposed query plans
     */
    PlacedDecomposedQueryPlans getAllQuerySubPlans();

    /**
     * Get the resources occupied by the decomposed query plans for the input query id.
     * @param sharedQueryId : the input shared query plan id
     */
    uint32_t getOccupiedResources(SharedQueryId sharedQueryId);

    /**
     * @brief Get identifier of all shared query plans placed on the execution node
     * @return set of shared query plan ids
     */
    std::set<SharedQueryId> getPlacedSharedQueryPlanIds();

    /**
     * @brief Get the amount of resources used by a specific sub plan
     * @param decomposedQueryPlan The decomposed query plan
     * @return the amount of resources
     */
    static uint32_t getOccupiedResourcesForDecomposedQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan);

    bool equal(NodePtr const& rhs) const override;

    std::string toString() const override;

    std::vector<std::string> toMultilineString() override;

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
     * a map of placed decomposed query plans
     */
    PlacedDecomposedQueryPlans mapOfSharedQueryToDecomposedQueryPlans;
};
}// namespace Optimizer
}// namespace NES

#endif// NES_OPTIMIZER_INCLUDE_PLANS_GLOBAL_EXECUTION_EXECUTIONNODE_HPP_
