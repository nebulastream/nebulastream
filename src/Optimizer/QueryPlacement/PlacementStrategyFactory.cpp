#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>

namespace NES {

std::unique_ptr<BasePlacementStrategy> PlacementStrategyFactory::getStrategy(std::string strategyName, GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan,
                                                                             TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    switch (stringToPlacementStrategyType[strategyName]) {
        case BottomUp:
            return BottomUpStrategy::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
        case TopDown:
            return TopDownStrategy::create(globalExecutionPlan, nesTopologyPlan, typeInferencePhase, streamCatalog);
            // FIXME: enable them with issue #755
            //        case LowLatency: return LowLatencyStrategy::create(nesTopologyPlan);
            //        case HighThroughput: return HighThroughputStrategy::create(nesTopologyPlan);
            //        case MinimumResourceConsumption: return MinimumResourceConsumptionStrategy::create(nesTopologyPlan);
            //        case MinimumEnergyConsumption: return MinimumEnergyConsumptionStrategy::create(nesTopologyPlan);
            //        case HighAvailability: return HighAvailabilityStrategy::create(nesTopologyPlan);
        default:
            return nullptr;
    }
}

}// namespace NES
