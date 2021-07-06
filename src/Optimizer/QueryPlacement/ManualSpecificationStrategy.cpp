#include <Optimizer/QueryPlacement/ManualSpecificationStrategy.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <utility>

namespace NES::Optimizer {

std::unique_ptr<ManualSpecificationStrategy>
NES::Optimizer::ManualSpecificationStrategy::create(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                    NES::TopologyPtr topology,
                                                    NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                    NES::StreamCatalogPtr streamCatalog) {
    return std::make_unique<ManualSpecificationStrategy>(ManualSpecificationStrategy(std::move(globalExecutionPlan),
                                                                                     std::move(topology),
                                                                                     std::move(typeInferencePhase),
                                                                                     std::move(streamCatalog)));
}

NES::Optimizer::ManualSpecificationStrategy::ManualSpecificationStrategy(NES::GlobalExecutionPlanPtr globalExecutionPlan,
                                                                         NES::TopologyPtr topology,
                                                                         NES::Optimizer::TypeInferencePhasePtr typeInferencePhase,
                                                                         NES::StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(std::move(globalExecutionPlan),
                            std::move(topology),
                            std::move(typeInferencePhase),
                            std::move(streamCatalog)) {}

bool NES::Optimizer::ManualSpecificationStrategy::updateGlobalExecutionPlan(NES::QueryPlanPtr queryPlan) {
    if (binaryMapping.empty()) {
        NES_ERROR("ManualSpecificationStrategy::updateGlobalExecutionPlan: binary mapping is not set");
        return false;
    }

    assignMappingToTopology(topology, queryPlan, this->binaryMapping);
    addNetworkSourceAndSinkOperators(queryPlan);
    runTypeInferencePhase(queryPlan->getQueryId());

    return true;
}
void ManualSpecificationStrategy::setBinaryMapping(std::vector<std::vector<bool>> userDefinedBinaryMapping) {
    this->binaryMapping = std::move(userDefinedBinaryMapping);
}

}// namespace NES::Optimizer
