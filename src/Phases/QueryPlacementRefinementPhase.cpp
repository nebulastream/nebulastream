#include <Phases/QueryPlacementRefinementPhase.hpp>

namespace NES {

QueryPlacementRefinementPhasePtr QueryPlacementRefinementPhase::create(GlobalExecutionPlanPtr globalPlan) {
    return std::make_shared<QueryPlacementRefinementPhase>(QueryPlacementRefinementPhase(globalPlan));
}

QueryPlacementRefinementPhase::QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan) {
    globalPlan = globalPlan;
    windowDistributionRefinement = WindowDistributionRefinement::create();
}

bool QueryPlacementRefinementPhase::execute(std::string queryId) {
    windowDistributionRefinement->execute(queryId);

    return true;
}

}// namespace NES