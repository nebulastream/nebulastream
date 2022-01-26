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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_

#include <Nodes/Node.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <z3++.h>

namespace z3 {
class expr;
class model;
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES

namespace NES::Optimizer {

/**
 * @brief This class implements Integer Linear Programming strategy to perform the operator placement
 */
class ILPStrategy : public BasePlacementStrategy {
  public:
    ~ILPStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan) override;

    static std::unique_ptr<ILPStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                               TopologyPtr topology,
                                               TypeInferencePhasePtr typeInferencePhase,
                                               SourceCatalogPtr streamCatalog,
                                               z3::ContextPtr z3Context);
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
    // default value for the relative weight of overutilization and network cost
    double overUtilizationCostWeight = 1.0;
    double networkCostWeight = 1.0;

    explicit ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                         TopologyPtr topology,
                         TypeInferencePhasePtr typeInferencePhase,
                         SourceCatalogPtr streamCatalog,
                         z3::ContextPtr z3Context);
    /**
     * @brief assigns operators to topology nodes based on ILP solution
     * @param queryPlan the query plan to place
     * @param z3Model a Z3 z3Model from the Z3 Optimize
     * @param placementVariables a mapping between concatenation of operator id and placement id and their z3 expression
     */
    bool placeOperators(QueryPlanPtr queryPlan, z3::model& z3Model, std::map<std::string, z3::expr>& placementVariables);

    /**
    * @brief Find a path between a source node and a destination node
    * @param sourceNode source operator or source topology node
    * @returns array containing all nodes on path from source to sink or parent topology node
     * TODO: #2290: try to extend existing function in the Topology class and use it instead
    */
    std::vector<NodePtr> findPathToRoot(NodePtr sourceNode);

    /**
    * @brief Populate the placement variables and adds constraints to the optimizer
    * @param context Z3 context
    * @param opt an instance of the Z3 optimize class
    * @param operatorNodePath the selected sequence of operator to add
    * @param topologyNodePath the selected sequence of topology node to add
    * @param operatorIdToNodeMap a mapping of operator id string and operator node object
    * @param topologyNodeIdToNodeMap a mapping of topology nodes id string and topology node object
    * @param placementVariable a mapping between concatenation of operator id and placement id and their z3 expression
    * @param operatorIdDistancesMap a mapping between operators (represented by ids) to their next operator in the topology
    * @param operatorIdUtilizationsMap a mapping of topology nodes and their node utilization
    * @param mileages a mapping of topology node (represented by string id) and their distance to the root node
    */
    bool addConstraints(z3::ContextPtr z3Context,
                        z3::optimize& opt,
                        std::vector<NodePtr>& operatorNodePath,
                        std::vector<NodePtr>& topologyNodePath,
                        std::map<std::string, OperatorNodePtr>& operatorIdToNodeMap,
                        std::map<std::string, TopologyNodePtr>& topologyNodeIdToNodeMap,
                        std::map<std::string, z3::expr>& placementVariable,
                        std::map<std::string, z3::expr>& operatorIdDistancesMap,
                        std::map<std::string, z3::expr>& operatorIdUtilizationsMap,
                        std::map<std::string, double>& mileages);

    /**
    * @brief calculates the mileage property for a node
    * @param node topology node for which mileage is calculated
    * @param mileages a mapping of topology node (represented by string id) and their distance to the root node
    */
    void computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileages);

    /**
    * @brief computes heuristics for distance
    * @param queryPlan query plan to place
    * @return a mapping of topology node (represented by string id) and their distance to the root node
    */
    std::map<std::string, double> computeDistanceHeuristic(QueryPlanPtr queryPlan);

    // context from the Z3 library used for optimization
    z3::ContextPtr z3Context;
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_QUERYPLACEMENT_ILPSTRATEGY_HPP_
