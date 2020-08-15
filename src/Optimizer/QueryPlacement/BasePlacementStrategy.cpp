#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Util/UtilityFunctions.hpp>
//#include <Optimizer/QueryPlacement/HighAvailabilityStrategy.hpp>
//#include <Optimizer/QueryPlacement/HighThroughputStrategy.hpp>
//#include <Optimizer/QueryPlacement/LowLatencyStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumEnergyConsumptionStrategy.hpp>
//#include <Optimizer/QueryPlacement/MinimumResourceConsumptionStrategy.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>
using namespace std;
namespace NES {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, TypeInferencePhasePtr typeInferencePhase,
                                             StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), nesTopologyPlan(nesTopologyPlan), typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog) {
    pathFinder = PathFinder::create(nesTopologyPlan);
}

uint64_t BasePlacementStrategy::addNetworkSinkOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr parentNesNode) {

    uint64_t queryId = queryPlan->getQueryId();
    uint64_t nextNetworkSourceOperatorId = -1;
    //FIXME: This need a second look when we will consider plans with multiple sources or merge operator.
    // Ideally we need independent sink and source operator pairs. This change is using existing source operator.
    // I will re-visit this in #750.
    NES_DEBUG("BasePlacementStrategy: Find the query sub plan of the parent node");
    const ExecutionNodePtr parentExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(parentNesNode->getId());
    if (parentExecutionNode && parentExecutionNode->hasQuerySubPlan(queryId)) {
        NES_DEBUG("BasePlacementStrategy: Found Query Sub Plan in the parent node");
        const QueryPlanPtr& parentQuerySubPlan = parentExecutionNode->getQuerySubPlan(queryId);
        NES_DEBUG("BasePlacementStrategy: Looking for existing network source operator in the parent query sub plan");
        const vector<SourceLogicalOperatorNodePtr> sourceOperators = parentQuerySubPlan->getSourceOperators();
        if (!sourceOperators.empty()) {
            const SourceLogicalOperatorNodePtr sourceOperator = sourceOperators[0];
            if(sourceOperator->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()){
                NES_DEBUG("BasePlacementStrategy: found existing network source operator in the parent execution node");
                nextNetworkSourceOperatorId = sourceOperator->getId();
            }
        }
    }

    if(nextNetworkSourceOperatorId == -1){
        NES_DEBUG("BasePlacementStrategy: found no existing network source operator in the parent execution node");
        nextNetworkSourceOperatorId = UtilityFunctions::getNextOperatorId();
    }

    uint64_t operatorId = UtilityFunctions::getNextOperatorId();
    Network::NodeLocation nodeLocation(parentNesNode->getId(), parentNesNode->getIpAddress(), parentNesNode->getDataPort());
    Network::NesPartition nesPartition(queryId, nextNetworkSourceOperatorId, nextNetworkSourceOperatorId, 0);
    const OperatorNodePtr sysSinkOperator = createSinkLogicalOperatorNode(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, NSINK_RETRY_WAIT, NSINK_RETRIES));
    sysSinkOperator->setId(operatorId);
    queryPlan->appendPreExistingOperator(sysSinkOperator);
    return nextNetworkSourceOperatorId;
}

void BasePlacementStrategy::addNetworkSourceOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr childNesNode, uint64_t operatorId) {

    if (operatorId == -1) {
        NES_ERROR("BasePlacementStrategy: received invalid operator id of the network source operator");
        throw QueryPlacementException("BasePlacementStrategy: Failed to add Network source operator due to invalid operator id -1");
    }

    uint64_t queryId = queryPlan->getQueryId();
    const ExecutionNodePtr childExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(childNesNode->getId());
    if (!(childExecutionNode)) {
        NES_ERROR("BasePlacementStrategy: Unable to find child execution node");
        throw QueryPlacementException("BasePlacementStrategy: Unable to find child execution node");
    }

    if (!childExecutionNode->hasQuerySubPlan(queryId)) {
        NES_ERROR("BasePlacementStrategy: Unable to find query sub plan in child execution node");
        throw QueryPlacementException("BasePlacementStrategy: Unable to find query sub plan in child execution node");
    }

    QueryPlanPtr childQuerySubPlan = childExecutionNode->getQuerySubPlan(queryId);
    if (!childQuerySubPlan) {
        NES_ERROR("BasePlacementStrategy: unable to find query sub plan with id " + queryId);
        throw QueryPlacementException("BasePlacementStrategy: unable to find query sub plan with id " + queryId);
    }
    vector<SinkLogicalOperatorNodePtr> sinkOperators = childQuerySubPlan->getSinkOperators();
    if (sinkOperators.empty()) {
        NES_ERROR("BasePlacementStrategy: Found a query sub plan without sink operator");
        throw QueryPlacementException("BasePlacementStrategy: Found a query sub plan without sink operator");
    }
    //If there are more than one sink operators then all of them should have the same output schema
    SchemaPtr sourceSchema = sinkOperators[0]->getOutputSchema();
    const Network::NesPartition nesPartition = Network::NesPartition(queryPlan->getQueryId(), operatorId, operatorId, 0);
    const OperatorNodePtr sysSourceOperator = createSourceLogicalOperatorNode(Network::NetworkSourceDescriptor::create(sourceSchema, nesPartition));
    sysSourceOperator->setId(operatorId);
    queryPlan->prependPreExistingOperator(sysSourceOperator);
}

// FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void BasePlacementStrategy::addSystemGeneratedOperators(uint64_t queryId, std::vector<NESTopologyEntryPtr> path) {
    NES_DEBUG("BasePlacementStrategy: Adding system generated operators");

    uint64_t nextNetworkSourceOperatorId = -1;
    auto pathItr = path.begin();
    NESTopologyEntryPtr previousNode;
    while (pathItr != path.end()) {

        NESTopologyEntryPtr currentNode = *pathItr;
        ExecutionNodePtr executionNode = globalExecutionPlan->getExecutionNodeByNodeId(currentNode->getId());
        if (!executionNode || !executionNode->hasQuerySubPlan(queryId)) {

            if (pathItr == path.begin()) {
                NES_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                          " This should not happen as placement at source node is necessary.");
                throw QueryPlacementException("BasePlacementStrategy: Unable to find execution node for source node!"
                                              " This should not happen as placement at source node is necessary.");
            }

            if (pathItr == path.end()) {
                NES_ERROR("BasePlacementStrategy: Unable to find execution node for sink node!"
                          " This should not happen as placement at sink node is necessary.");
                throw QueryPlacementException("BasePlacementStrategy: Unable to find execution node for sink node!"
                                              " This should not happen as placement at sink node is necessary.");
            }

            //create a new execution node for the nes node with forward operators
            if (!executionNode) {
                NES_DEBUG("BasePlacementStrategy: Create a new execution node");
                executionNode = ExecutionNode::createExecutionNode(currentNode);
            }

            QueryPlanPtr querySubPlan = QueryPlan::create();
            querySubPlan->setQueryId(queryId);
            NESTopologyEntryPtr childNesNode = *(pathItr - 1);
            addNetworkSourceOperator(querySubPlan, childNesNode, nextNetworkSourceOperatorId);
            NESTopologyEntryPtr parentNesNode = *(pathItr + 1);
            nextNetworkSourceOperatorId = addNetworkSinkOperator(querySubPlan, parentNesNode);

            NES_DEBUG("BasePlacementStrategy: Infer the output and input schema for the updated query plan");
            typeInferencePhase->execute(querySubPlan);

            if (!executionNode->createNewQuerySubPlan(queryId, querySubPlan)) {
                NES_ERROR("BasePlacementStrategy: Unable to add system generated query sub plan.");
                throw QueryPlacementException("BasePlacementStrategy: Unable to add system generated query sub plan.");
            }

            if (!globalExecutionPlan->addExecutionNodeAsParentTo(childNesNode->getId(), executionNode)) {
                NES_ERROR("BasePlacementStrategy: Unable to add execution node with forward operators");
                throw QueryPlacementException("BasePlacementStrategy: Unable to add execution node with forward operators");
            }

        } else {
            const QueryPlanPtr querySubPlan = executionNode->getQuerySubPlan(queryId);
            if (!querySubPlan) {
                NES_ERROR("BasePlacementStrategy: unable to find query sub plan with id " + queryId);
                throw QueryPlacementException("BasePlacementStrategy: unable to find query sub plan with id " + queryId);
            }

            querySubPlan->setQueryId(queryId);

            if (querySubPlan->getSinkOperators().empty()) {
                if (pathItr == path.end()) {
                    NES_ERROR("BasePlacementStrategy: Unable to find sink operator at the sink node!"
                              " This should not happen as placement at sink node is necessary.");
                    throw QueryPlacementException("BasePlacementStrategy: Unable to find sink operator at the sink node!"
                                                  " This should not happen as placement at sink node is necessary.");
                }
                NESTopologyEntryPtr parentNesNode = *(pathItr + 1);
                nextNetworkSourceOperatorId = addNetworkSinkOperator(querySubPlan, parentNesNode);
            }

            if (querySubPlan->getSourceOperators().empty()) {
                if (pathItr == path.begin()) {
                    NES_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                              " This should not happen as placement at source node is necessary.");
                    throw QueryPlacementException("BasePlacementStrategy: Unable to find execution node for source node!"
                                                  " This should not happen as placement at source node is necessary.");
                }
                NESTopologyEntryPtr childNesNode = *(pathItr - 1);
                addNetworkSourceOperator(querySubPlan, childNesNode, nextNetworkSourceOperatorId);
            }

            NES_DEBUG("BasePlacementStrategy: Infer the output and input schema for the updated query plan");
            typeInferencePhase->execute(querySubPlan);

            if (!executionNode->updateQuerySubPlan(queryId, querySubPlan)) {
                NES_ERROR("BasePlacementStrategy: Unable to add system generated source operator.");
                throw QueryPlacementException("BasePlacementStrategy: Unable to add system generated source operator.");
            }
        }

        if (previousNode) {
            NES_DEBUG("BasePlacementStrategy: adding link between previous and current execution node");
            if (!globalExecutionPlan->addExecutionNodeAsParentTo(previousNode->getId(), executionNode)) {
                NES_ERROR("BasePlacementStrategy: Unable to add link between previous and current executionNode");
                throw QueryPlacementException("BasePlacementStrategy: Unable to add link between previous and current executionNode");
            }
        }
        NES_DEBUG("BasePlacementStrategy: scheduling execution node");
        globalExecutionPlan->scheduleExecutionNode(executionNode);
        previousNode = (*pathItr);
        ++pathItr;
    }
    NES_DEBUG("BasePlacementStrategy: Finished added system generated operators");
}

}// namespace NES
