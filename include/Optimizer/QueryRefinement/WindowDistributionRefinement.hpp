#ifndef NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_

#include <Optimizer/QueryRefinement/BaseRewriteRule.hpp>

namespace NES {

class Node;
class WindowDistributionRefinement;
typedef std::shared_ptr<WindowDistributionRefinement> WindowDistributionRefinementPtr;

/**
 * @brief This class is responsible for adding system generated operators once data have to be transferred from one node to another
 * @rule current rule, if global executionPlan has more than three nodes, use the distributed window operator
 */
class WindowDistributionRefinement : public BaseRewriteRule {

  public:
    /**
     * @brief method to execute a refinement rule on the global plan that target only nodes and operators of a specific query
     * @param globalPlan
     * @param queryId
     * @return bool indicating success
     */
    bool execute(GlobalExecutionPlanPtr globalPlan, QueryId queryId) override;

    /**
     * @brief mehtod to create a refinement rule
     * @return
     */
    static WindowDistributionRefinementPtr create();

  private:
    explicit WindowDistributionRefinement();
};

}// namespace NES
#endif//NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_CHOOSEWINDOWDISTRIBUTION_HPP_
