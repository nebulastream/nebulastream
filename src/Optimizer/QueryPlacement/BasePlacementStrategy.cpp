#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/UtilityFunctions.hpp>
//#include <Optimizer/QueryPlacement/HighAvailabilityStrategy.hpp>
//#include <Optimizer/QueryPlacement/HighThroughputStrategy.hpp>
//#include <Optimizer/QueryPlacement/LowLatencyStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumEnergyConsumptionStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumResourceConsumptionStrategy.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>

using namespace std;
namespace NES {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topologyPtr, TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), topology(topologyPtr), typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog) {}

uint64_t BasePlacementStrategy::addNetworkSinkOperator(QueryPlanPtr queryPlan, TopologyNodePtr parentNesNode) {

    NES_DEBUG("BasePlacementStrategy: found no existing network source operator in the parent execution node");
    QueryId queryId = queryPlan->getQueryId();

    uint64_t operatorId = UtilityFunctions::getNextOperatorId();
    uint64_t nextNetworkSourceOperatorId = UtilityFunctions::getNextOperatorId();
    Network::NodeLocation nodeLocation(parentNesNode->getId(), parentNesNode->getIpAddress(), parentNesNode->getDataPort());
    Network::NesPartition nesPartition(queryId, nextNetworkSourceOperatorId, nextNetworkSourceOperatorId, 0);
    const OperatorNodePtr sysSinkOperator = createSinkLogicalOperatorNode(Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
    sysSinkOperator->setId(operatorId);
    queryPlan->appendPreExistingOperator(sysSinkOperator);
    return nextNetworkSourceOperatorId;
}

void BasePlacementStrategy::addNetworkSourceOperator(QueryPlanPtr queryPlan, SchemaPtr inputSchema, uint64_t operatorId) {
    const Network::NesPartition nesPartition = Network::NesPartition(queryPlan->getQueryId(), operatorId, operatorId, 0);
    const OperatorNodePtr sysSourceOperator = createSourceLogicalOperatorNode(Network::NetworkSourceDescriptor::create(inputSchema, nesPartition));
    sysSourceOperator->setId(operatorId);
    queryPlan->appendPreExistingOperator(sysSourceOperator);
}

}// namespace NES
