//
// Created by andre on 26.06.2021.
//

#ifndef GRADIENTSTRATEGY_HPP
#define GRADIENTSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <torch/torch.h>
#include <iostream>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;
}

namespace NES::Optimizer {

/**\brief:
 *          This class implements Gradient Descent placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes. The placements for other operators are computed iteratively. The results of the
 *          computations determine a valid placement for each operator.
 */

class GradientStrategy : public BasePlacementStrategy {
  public:
    ~BottomUpStrategy() override = default;

    bool updateGlobalExecutionPlan(QueryPlanPtr queryPlan);

    static std::unique_ptr<GradientStrategy> create(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                                                    TopologyPtr topology,
                                                    TypeInferencePhasePtr typeInferencePhase,
                                                    StreamCatalogPtr streamCatalog);

  private:
    explicit GradientStrategy(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                              TopologyPtr topology,
                              TypeInferencePhasePtr typeInferencePhase,
                              StreamCatalogPtr streamCatalog);

    void placeQueryPlanOnTopology(const QueryPlanPtr& queryPlan);

    std::vector<NodePtr> findPathToRoot(NodePtr sourceNode);
    torch::Tensor computePlacement(std::vector<NodePtr> operators, std::vector<NodePtr> nodes);
    at::Tensor finalize(at::Tensor tensor);


    torch::Tensor operatorPlacement(torch::Tensor assignmentMatrix,
                                    torch::Tensor operatorCostMatrix,
                                    torch::Tensor finalizedMatrix,
                                    std::vector<NodePtr> nodes,
                                    int opIndex);

    torch::Tensor assignCostParameter(std::vector<NodePtr> vector);

    void reduceResources(std::vector<NodePtr> nodes, at::Tensor placementTensor);

    std::map<int, std::vector<int>>
    operatorAssignment(std::vector<NodePtr> operators, std::vector<NodePtr> nodes, torch::Tensor finalizedMatrix);

    void finalizePlacement(const QueryPlanPtr& queryPlan, std::map<int, std::vector<int>> assignedOperators);
    std::map<int, std::vector<int>> modifyMapping(std::map<int, std::vector<int>> assignedOperators,
                                                  std::map<int, std::vector<int>> input);
};

}

#endif//NES_GRADIENTSTRATEGY_HPP
