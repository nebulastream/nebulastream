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
    explicit ILPStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);

    void placeOperators();
    std::vector<NodePtr> findPathToRoot(NodePtr sourceNode);
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
    void computeDistanceRecursive(TopologyNodePtr node, std::map<std::string, double>& mileageMap);
    std::map<std::string, double> computeDistanceHeuristic(QueryPlanPtr queryPlan);
    void applyOperatorHeuristics(QueryPlanPtr queryPlan);
    void assignOperatorPropertiesRecursive(OperatorNodePtr operatorNode, double input);
};
}// namespace NES::Optimizer

#endif//NES_ILPSTRATEGY_HPP
