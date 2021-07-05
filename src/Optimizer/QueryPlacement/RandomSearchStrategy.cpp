#include <Optimizer/QueryPlacement/RandomSearchStrategy.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<RandomSearchStrategy>
NES::Optimizer::RandomSearchStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                             NES::TopologyPtr topology,
                                             NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                             NES::StreamCatalogPtr streamCatalog) {
    return std::make_unique<RandomSearchStrategy>(RandomSearchStrategy(std::move(globalExecutionPlan),
                                                                       std::move(topology),
                                                                       std::move(typeInferencePhase),
                                                                       std::move(streamCatalog)));
}

NES::Optimizer::RandomSearchStrategy::RandomSearchStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                           NES::TopologyPtr topology,
                                                           NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                           NES::StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool NES::Optimizer::RandomSearchStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {

    std::vector<std::vector<bool>> placementMatrix = {{true, false, false}, {false, true, false}, {false, false, true}};

    assignMappingToTopology(topology, queryPlan, placementMatrix);

    addNetworkSourceAndSinkOperators(queryPlan);

    runTypeInferencePhase(queryPlan->getQueryId());

    return true;
}

}// namespace NES::Optimizer
