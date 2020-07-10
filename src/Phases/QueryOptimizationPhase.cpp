#include <Catalogs/QueryCatalogEntry.hpp>
#include <Phases/QueryOptimizationPhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/QueryPlacementPhase.hpp>

namespace NES {

QueryOptimizationPhase::QueryOptimizationPhase(QueryRewritePhasePtr queryRewritePhase, TypeInferencePhasePtr typeInferencePhase,
                                               QueryPlacementPhasePtr queryPlacementPhase)
    : queryRewritePhase(queryRewritePhase), typeInferencePhase(typeInferencePhase), queryPlacementPhase(queryPlacementPhase) {
}

QueryOptimizationPhasePtr QueryOptimizationPhase::create(QueryRewritePhasePtr queryRewritePhase, TypeInferencePhasePtr typeInferencePhase,
                                                         QueryPlacementPhasePtr queryPlacementPhase) {
    return std::make_shared<QueryOptimizationPhase>(QueryOptimizationPhase(queryRewritePhase, typeInferencePhase, queryPlacementPhase));
}

bool QueryOptimizationPhase::execute(QueryCatalogEntryPtr queryCatalogEntry) {


}
}// namespace NES