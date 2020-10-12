#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Phases/QueryMergerPhase.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryMergerPhasePtr QueryMergerPhase::create() {
    return std::make_shared<QueryMergerPhase>(QueryMergerPhase());
}

QueryMergerPhase::QueryMergerPhase() {
    this->l0QueryMergerRule = L0QueryMergerRule::create();
}

bool QueryMergerPhase::execute(GlobalQueryPlanPtr globalQueryPlan) {
    NES_DEBUG("QueryMergerPhase: Executing query merger phase.");
    return l0QueryMergerRule->apply(globalQueryPlan);
}

}// namespace NES
