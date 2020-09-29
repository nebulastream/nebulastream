#include <Optimizer/QueryMerger/L0QueryMergerRule.hpp>
#include <Phases/QueryMergerPhase.hpp>

namespace NES {

QueryMergerPhasePtr QueryMergerPhase::create() {
    return std::make_shared<QueryMergerPhase>(QueryMergerPhase());
}

QueryMergerPhase::QueryMergerPhase() {
    this->l0QueryMergerRule = L0QueryMergerRule::create();
}

bool QueryMergerPhase::execute(GlobalQueryPlanPtr globalQueryPlan) {
    l0QueryMergerRule->apply(globalQueryPlan);
    return true;
}

}// namespace NES
