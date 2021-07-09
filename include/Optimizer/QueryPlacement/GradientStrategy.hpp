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
    ~GradientStrategy(){};

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

    torch::Tensor paramWeights = torch::Tensor([4.0, 5.0, 20.0, 10.0])


    void createOperatorCostTensor(QueryPlanPtr queryPlan);

    void createTopologyMatrix(TopologyPtr topology);

    void createFinalizedMatrix(TopologyPtr topology);

    void operatorPlacement(QueryPlanPtr queryPlan, int operatorIndex, torch::Tensor topologyMatrix, torch::Tensor costTensor);

    void createOperatorCostTensor(QueryPlanPtr queryPlan);
};

}

#endif//NES_GRADIENTSTRATEGY_HPP
