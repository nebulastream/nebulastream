//
// Created by andre on 26.06.2021.
//

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <utility>
#include <Optimizer/QueryPlacement/GradientStrategy.hpp>

namespace NES::Optimizer {

std::unique_ptr<GradientStrategy> GradientStrategy::create(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                                                           TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase,
                                                           StreamCatalogPtr streamCatalog) {
    return std::unique_ptr<GradientStrategy>();
}


GradientStrategy::GradientStrategy(GlobalExecutionPlanPtr globalExecutionPlanPtr,
                                   TopologyPtr topology,
                                   TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlanPtr, topology, typeInferencePhase, streamCatalog) {}

bool GradientStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) { return false; }
void GradientStrategy::createOperatorCostTensor(QueryPlanPtr queryPlan) {}
void GradientStrategy::createTopologyMatrix(TopologyPtr topology) {}
void GradientStrategy::createFinalizedMatrix(TopologyPtr topology) {}
void GradientStrategy::operatorPlacement(QueryPlanPtr queryPlan,
                                         int operatorIndex,
                                         torch::Tensor topologyMatrix,
                                         torch::Tensor costTensor) {}
}
