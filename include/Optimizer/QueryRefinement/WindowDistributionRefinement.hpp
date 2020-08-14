#ifndef NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_

#include <Optimizer/QueryRefinement/BaseRefinementRule.hpp>

namespace NES {

class Node;
class WindowDistributionRefinement;
typedef std::shared_ptr<WindowDistributionRefinement> WindowDistributionRefinementPtr;

/**
 * @brief This class is responsible for adding system generated operators once data have to be transferred from one node to another
 * @rule current rule, if global executionPlan has more than three nodes, use the distributed window operator
 */
class WindowDistributionRefinement : public BaseRefinementRule {

  public:
    bool execute(GlobalExecutionPlanPtr globalPlan, std::string queryId) override;

    static WindowDistributionRefinementPtr create();

  private:
    explicit WindowDistributionRefinement();

};

}// namespace NES
#endif//NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_CHOOSEWINDOWDISTRIBUTION_HPP_
