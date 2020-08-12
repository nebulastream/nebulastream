#ifndef NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_WindowDistributionRefinement_HPP_

#include <Optimizer/QueryRefinement/BaseRefinementRule.hpp>

namespace NES {

class Node;
class WindowDistributionRefinement;
typedef std::shared_ptr<WindowDistributionRefinement> WindowDistributionRefinementPtr;

/**
 * @brief This class is responsible for adding system generated operators once data have to be transfered from one node to another
 */
class WindowDistributionRefinement : public BaseRefinementRule {

  public:
    void execute(std::string queryId) override;

    static WindowDistributionRefinementPtr create();

  private:
    explicit WindowDistributionRefinement();

};

}// namespace NES
#endif//NES_INCLUDE_OPTIMIZER_QUERYREFINEMENT_CHOOSEWINDOWDISTRIBUTION_HPP_
