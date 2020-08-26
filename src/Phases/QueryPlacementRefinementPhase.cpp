#include <Phases/QueryPlacementRefinementPhase.hpp>

namespace NES {

QueryPlacementRefinementPhasePtr QueryPlacementRefinementPhase::create(GlobalExecutionPlanPtr globalPlan) {
    return std::make_shared<QueryPlacementRefinementPhase>(QueryPlacementRefinementPhase(globalPlan));
}

QueryPlacementRefinementPhase::~QueryPlacementRefinementPhase()
{
    NES_DEBUG("~QueryPlacementRefinementPhase()");
}
QueryPlacementRefinementPhase::QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan) {
    NES_DEBUG("QueryPlacementRefinementPhase()");
    globalExecutionPlan = globalPlan;
    windowDistributionRefinement = WindowDistributionRefinement::create();
}

bool QueryPlacementRefinementPhase::execute(QueryId queryId) {
    windowDistributionRefinement->execute(globalExecutionPlan, queryId);
    return true;
}

}// namespace NES