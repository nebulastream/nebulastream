//
// Created by Jule on 26.05.2021.
//

#ifndef NES_ILPSTRATEGY_HPP
#define NES_ILPSTRATEGY_HPP

#include <Nodes/Node.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <z3++.h>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
}// namespace NES

namespace NES::Optimizer {

/**\brief:
 *          This class implements ILP strategy.
 */
class ILPStrategy : public BasePlacementStrategy {
  public:
    ~ILPStrategy(){};

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    static std::unique_ptr<ILPStrategy> create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                    TopologyPtr topology,
                                                    TypeInferencePhasePtr typeInferencePhase,
                                                    StreamCatalogPtr streamCatalog);

  private:
    double weightOverutilization = 1.0;
    double weightNetworkCost = 1.0;

    explicit ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);
    /**
     * assigns operators to topology nodes based on ILP solution
     * @param queryPlan
     * @param m
     * @param placementVariables
     */
    void placeOperators(QueryPlanPtr queryPlan, z3::model& m, std::map<std::string, z3::expr>& placementVariables);

    /**
    * @param sourceNode source operator or source topology node
    * @returns array containing all nodes on path from source to sink or parent topology node
    */
    std::vector<NodePtr> findPathToRoot(NodePtr sourceNode);

    /**
    * creates the placement variables and adds constraints to the optimizer
    * @param c Z3 context
    * @param opt
    * @param operatorPath
    * @param topologyPath
    * @param operatorNodes
    * @param topologyNodes
    * @param placementVariables
    * @param positions
    * @param utilizations
    * @param mileages
    */
    void addPath(z3::context& c,
                 z3::optimize& opt,
                 std::vector<NodePtr>& operatorPath,
                 std::vector<NodePtr>& topologyPath,
                 std::map<std::string, OperatorNodePtr>& operatorNodes,
                 std::map<std::string, TopologyNodePtr>& topologyNodes,
                 std::map<std::string, z3::expr>& placementVariables,
                 std::map<std::string, z3::expr>& distances,
                 std::map<std::string, z3::expr>& utilizations,
                 std::map<std::string, double>& mileages);

    /**
    * calculates the mileage property for a node
    * mileage: distance to the root node, takes into account the bandwidth of the links
    * @param node topology node for which mileage is calculated
    * @mileageMap map of mileages
    */
    void computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileageMap);

    /**
    * computes heuristics for distance
    * @param queryPlan
    * @return the map of mileage parameters
    */
    std::map<std::string, double> computeDistanceHeuristic(QueryPlanPtr queryPlan);

    /**
     * assigns the output and cost properties to each operator
     * @param queryPlan
     */
    void applyOperatorHeuristics(QueryPlanPtr queryPlan);

    /**
    * called by applyOperatorHeuristics, (if not provided) estimates output and computing-cost of an
    * @param operatorNode
    */
    void assignOperatorPropertiesRecursive(LogicalOperatorNodePtr operatorNode);

    double getOUWeight();
    double getNetWeight();
    void setOUWeight(double value);
    void setNetWeight(double value);
};
}// namespace NES::Optimizer

#endif//NES_ILPSTRATEGY_HPP
