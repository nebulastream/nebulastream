#include <Phases/QueryPlacementRefinementPhase.hpp>
#include <Util/Logger.hpp>
namespace NES {

QueryPlacementRefinementPhasePtr QueryPlacementRefinementPhase::create(GlobalExecutionPlanPtr globalPlan) {
    return std::make_shared<QueryPlacementRefinementPhase>(QueryPlacementRefinementPhase(globalPlan));
}

QueryPlacementRefinementPhase::~QueryPlacementRefinementPhase() {
    NES_DEBUG("~QueryPlacementRefinementPhase()");
}
QueryPlacementRefinementPhase::QueryPlacementRefinementPhase(GlobalExecutionPlanPtr globalPlan) {
    NES_DEBUG("QueryPlacementRefinementPhase()");
    globalExecutionPlan = globalPlan;
}

bool QueryPlacementRefinementPhase::execute(QueryId queryId) {
    NES_DEBUG("QueryPlacementRefinementPhase() execute for query " << queryId);
    return true;
}

}// namespace NES