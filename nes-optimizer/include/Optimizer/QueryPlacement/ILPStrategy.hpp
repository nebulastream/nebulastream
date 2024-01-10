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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_

#include <Nodes/Node.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <z3++.h>

namespace z3 {
class expr;
class model;
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Catalogs::Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES::Catalogs::Source

namespace NES::Optimizer {

/**
 * @brief This class implements Integer Linear Programming strategy to perform the operator placement
 */
class ILPStrategy : public BasePlacementStrategy {
  public:
    ~ILPStrategy() override = default;

    bool updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                   const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::set<LogicalOperatorNodePtr>& pinnedDownStreamOperators) override;

    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmenderMode placementAmenderMode);

    /**
     * @brief set the relative weight for the overutilization cost to be used when computing weighted sum in the final cost
     * @param weight the relative weight
     */
    void setOverUtilizationWeight(double weight);

    /**
     * @brief get the relative weight for the overutilization cost
     * @return the relative weight for the overutilization cost
     */
    double getOverUtilizationCostWeight();

    /**
     * @brief set the relative weight for the network cost to be used when computing weighted sum in the final cost
     * @param weight the relative weight
     */
    void setNetworkCostWeight(double weight);

    /**
     * @brief get the relative weight for the network cost
     * @return the relative weight for the network cost
     */
    double getNetworkCostWeight();

  private:
    // default weights for over utilization and network cost
    double overUtilizationCostWeight = 1.0;
    double networkCostWeight = 1.0;
    // context from the Z3 library used for optimization
    z3::ContextPtr z3Context;
    //map to hold operators to place
    std::map<OperatorId, LogicalOperatorNodePtr> operatorMap;
    const char* const KEY_SEPARATOR = ",";

    explicit ILPStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                         const TopologyPtr& topology,
                         const TypeInferencePhasePtr& typeInferencePhase,
                         const z3::ContextPtr& z3Context,
                         PlacementAmenderMode placementAmenderMode);
    /**
     * @brief assigns operators to topology nodes based on ILP solution
     * @param z3Model a Z3 z3Model from the Z3 Optimize
     * @param placementVariables a mapping between concatenation of operator id and placement id and their z3 expression
     */
    bool pinOperators(z3::model& z3Model, std::map<std::string, z3::expr>& placementVariables);

    /**
    * @brief Populate the placement variables and adds constraints to the optimizer
    * @param opt an instance of the Z3 optimize class
    * @param operatorNodePath the selected sequence of operator to add
    * @param topologyNodePath the selected sequence of topology node to add
    * @param placementVariable a mapping between concatenation of operator id and placement id and their z3 expression
    * @param operatorDistanceMap a mapping between operators (represented by ids) to their next operator in the topology
    * @param nodeUtilizationMap a mapping of topology nodes and their node utilization
    * @param nodeMileageMap a mapping of topology node (represented by string id) and their distance to the root node
    */
    void addConstraints(z3::optimize& opt,
                        std::vector<NodePtr>& operatorNodePath,
                        std::vector<TopologyNodePtr>& topologyNodePath,
                        std::map<std::string, z3::expr>& placementVariable,
                        std::map<OperatorId, z3::expr>& operatorDistanceMap,
                        std::map<uint64_t, z3::expr>& nodeUtilizationMap,
                        std::map<uint64_t, double>& nodeMileageMap);

    /**
    * @brief computes heuristics for distance
    * @param pinnedUpStreamOperators: pinned upstream operators
    * @return a mapping of topology node (represented by string id) and their distance to the root node
    */
    std::map<uint64_t, double> computeMileage(const std::set<LogicalOperatorNodePtr>& pinnedUpStreamOperators);

    /**
    * @brief calculates the mileage property for a node
    * @param node topology node for which mileage is calculated
    * @param mileages a mapping of topology node (represented by string id) and their distance to the root node
    */
    void computeDistance(const TopologyNodePtr& node, std::map<uint64_t, double>& mileages);

    /**
     * Get default operator output value
     * @param operatorNode : the operator for which output values are needed
     * @return weight for the output
     */
    double getDefaultOperatorOutput(const LogicalOperatorNodePtr& operatorNode);

    /**
     * Get default value for operator cost
     * @param operatorNode : operator for which cost is to be computed
     * @return weight indicating operator cost
     */
    int getDefaultOperatorCost(const LogicalOperatorNodePtr& operatorNode);
};
}// namespace NES::Optimizer

#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_
