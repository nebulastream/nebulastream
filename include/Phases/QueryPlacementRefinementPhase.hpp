#ifndef NES_QueryPlacementRefinementPhase_HPP
#define NES_QueryPlacementRefinementPhase_HPP

#include <memory>
#include <Optimizer/QueryRefinement/WindowDistributionRefinement.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>

namespace NES {
class QueryPlacementRefinementPhase;
typedef std::shared_ptr<QueryPlacementRefinementPhase> QueryPlacementRefinementPhasePtr;

/**
 * @brief This phase is responsible for refinement of the query plan
 */
class QueryPlacementRefinementPhase {
  public:
    static QueryPlacementRefinementPhasePtr create(GlobalExecutionPlanPtr globalPlan);

    /**
     * @brief apply changes to the global query plan
     * @param queryId
     * @return success
     */
    bool execute(std::string queryId);

  private:
    explicit QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan);
    WindowDistributionRefinementPtr windowDistributionRefinement;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES
#endif//NES_QueryPlacementRefinementPhase_HPP
