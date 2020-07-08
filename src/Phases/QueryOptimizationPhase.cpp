#include <Phases/QueryOptimizationPhase.hpp>

namespace NES {

QueryOptimizationPhasePtr QueryOptimizationPhase::create() {
    return std::make_shared<QueryOptimizationPhase>(QueryOptimizationPhase());
}

bool QueryOptimizationPhase::execute(QueryCatalogEntryPtr queryCatalogEntry) {

}
}// namespace NES