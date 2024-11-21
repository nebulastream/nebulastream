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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ILPSTRATEGY_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ILPSTRATEGY_HPP_

#include <Nodes/Node.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
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
class ILPStrategy : public BasePlacementAdditionStrategy {
  public:
    ~ILPStrategy() override = default;

    static BasePlacementStrategyPtr create(const GlobalExecutionPlanPtr& globalExecutionPlan,
                                           const TopologyPtr& topology,
                                           const TypeInferencePhasePtr& typeInferencePhase,
                                           PlacementAmendmentMode placementAmendmentMode);

    PlacementAdditionResult updateGlobalExecutionPlan(SharedQueryId sharedQueryId,
                                                      const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                                                      const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                                      DecomposedQueryPlanVersion querySubPlanVersion, FaultToleranceType faultToleranceType = FaultToleranceType::NONE) override;

    /**
     * @brief set the relative weight for the overutilization cost to be used when computing weighted sum in the final cost
     * @param weight the relative weight
     */
    void setOverUtilizationWeight(double weight);

    /**
     * @brief get the relative weight for the overutilization cost
     * @return the relative weight for the overutilization cost
     */
    double getOverUtilizationCostWeight() const;

    /**
     * @brief set the relative weight for the network cost to be used when computing weighted sum in the final cost
     * @param weight the relative weight
     */
    void setNetworkCostWeight(double weight);

    /**
     * @brief get the relative weight for the network cost
     * @return the relative weight for the network cost
     */
    double getNetworkCostWeight() const;

  private:
    // default weights for over utilization and network cost
    double overUtilizationCostWeight = 1.0;
    double networkCostWeight = 1.0;
    // context from the Z3 library used for optimization
    z3::ContextPtr z3Context;
    //map to hold operators to place
    std::map<OperatorId, LogicalOperatorPtr> operatorMap;
    const char* const KEY_SEPARATOR = ",";

    explicit ILPStrategy(const GlobalExecutionPlanPtr& globalExecutionPlan,
                         const TopologyPtr& topology,
                         const TypeInferencePhasePtr& typeInferencePhase,
                         const z3::ContextPtr& z3Context,
                         PlacementAmendmentMode placementAmendmentMode);
    /**
     * @brief assigns operators to topology nodes based on ILP solution
     * @param z3Model a Z3 z3Model from the Z3 Optimize
     * @param placementVariables a mapping between concatenation of operator id and placement id and their z3 expression
     */
    bool pinOperators(z3::model& z3Model, std::map<std::string, z3::expr>& placementVariables);

    /**
     * @brief is called from addConstraints and calling itself recursivly with parents of operator Node to identify their location on topologyPath
     * @param logicalOperator the current logical operator
     * @param opt an instance of the Z3 optimize class
     * @param placementVariable a mapping between concatenation of operator id and placement id and their z3 expression
     * @param pinnedDownStreamOperators
     * @param operatorDistanceMap a mapping between operators (represented by ids) to their next operator in the topology
     * @param nodeUtilizationMap a mapping of topology nodes and their node utilization
     * @param nodeMileageMap a mapping of topology node (represented by string id) and their distance to the root node
     */
    void identifyPinningLocation(const LogicalOperatorPtr& logicalOperator,
                                 z3::optimize& opt,
                                 std::unordered_map<OperatorId, std::vector<WorkerId>> potentialOperatorPinningLocation,
                                 std::map<std::string, z3::expr>& placementVariable,
                                 const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                                 std::map<OperatorId, z3::expr>& operatorDistanceMap,
                                 std::map<WorkerId, z3::expr>& nodeUtilizationMap,
                                 std::map<WorkerId, double>& nodeMileageMap);

    /**
     * @brief Populate the placement variables and adds constraints to the optimizer
     * @param opt an instance of the Z3 optimize class
     * @param pinnedUpStreamOperators
     * @param pinnedDownStreamOperators
     * @param placementVariable a mapping between concatenation of operator id and placement id and their z3 expression
     * @param operatorDistanceMap a mapping between operators (represented by ids) to their next operator in the topology
     * @param nodeUtilizationMap a mapping of topology nodes and their node utilization
     * @param nodeMileageMap a mapping of topology node (represented by string id) and their distance to the root node
     */
    void addConstraints(z3::optimize& opt,
                        const std::set<LogicalOperatorPtr>& pinnedUpStreamOperators,
                        const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators,
                        std::map<std::string, z3::expr>& placementVariable,
                        std::map<OperatorId, z3::expr>& operatorDistanceMap,
                        std::map<WorkerId, z3::expr>& nodeUtilizationMap,
                        std::map<WorkerId, double>& nodeMileageMap);

    /**
     * @brief computes heuristics for distance
     * @param pinnedDownStreamOperators: pinned downstream operators
     * @return a mapping of topology node (represented by string id) and their distance to the root node
     */
    std::map<WorkerId, double> computeMileage(const std::set<LogicalOperatorPtr>& pinnedDownStreamOperators);

    /**
     * @brief calculates the mileage property for a node
     * @param node topology node for which mileage is calculated
     * @param mileages a mapping of topology node (represented by string id) and their distance to the root node
     */
    void computeDistance(const TopologyNodePtr& node, std::map<WorkerId, double>& mileages);

    /**
     * Assign default cost values
     * @param operatorNode : the last operator to start with
     */
    void assignOperatorDefaultProperties(const LogicalOperatorPtr& operatorNode);
};
}// namespace NES::Optimizer

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYPLACEMENTADDITION_ILPSTRATEGY_HPP_
