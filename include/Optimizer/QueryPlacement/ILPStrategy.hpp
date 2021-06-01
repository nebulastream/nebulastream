//
// Created by Jule on 26.05.2021.
//

#ifndef NES_ILPSTRATEGY_HPP
#define NES_ILPSTRATEGY_HPP

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>

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
};
}// namespace NES::Optimizer

#endif//NES_ILPSTRATEGY_HPP
