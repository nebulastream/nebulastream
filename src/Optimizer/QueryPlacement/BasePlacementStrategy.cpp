#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TypeInferencePhase.hpp>
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

namespace NES {

BasePlacementStrategy::BasePlacementStrategy(GlobalExecutionPlanPtr globalExecutionPlan, NESTopologyPlanPtr nesTopologyPlan, TypeInferencePhasePtr typeInferencePhase,
                                             StreamCatalogPtr streamCatalog)
    : globalExecutionPlan(globalExecutionPlan), nesTopologyPlan(nesTopologyPlan), typeInferencePhase(typeInferencePhase), streamCatalog(streamCatalog) {
    pathFinder = PathFinder::create(nesTopologyPlan);
}

OperatorNodePtr BasePlacementStrategy::createNetworkSinkOperator(NESTopologyEntryPtr nesNode) {
    auto sinkOperator = createSinkLogicalOperatorNode(ZmqSinkDescriptor::create(nesNode->getIp(), ZMQ_DEFAULT_PORT));
    sinkOperator->setId(SYS_SINK_OPERATOR_ID);// all sink operators will have MAX64-1 as Id
    return sinkOperator;
}

OperatorNodePtr BasePlacementStrategy::createNetworkSourceOperator(NESTopologyEntryPtr nesNode, SchemaPtr schema) {
    auto sourceOperator = createSourceLogicalOperatorNode(ZmqSourceDescriptor::create(schema, nesNode->getIp(), ZMQ_DEFAULT_PORT));
    sourceOperator->setId(SYS_SOURCE_OPERATOR_ID);// all source operators will have MAX64-2 as Id
    return sourceOperator;
}

void BasePlacementStrategy::addNetworkSinkOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr parentNesNode) {
    const OperatorNodePtr sysSinkOperator = createNetworkSinkOperator(parentNesNode);
    queryPlan->appendPreExistingOperator(sysSinkOperator);
}

void BasePlacementStrategy::addNetworkSourceOperator(QueryPlanPtr queryPlan, NESTopologyEntryPtr currentNesNode, NESTopologyEntryPtr childNesNode) {
    std::string queryId = queryPlan->getQueryId();
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
    SchemaPtr sourceSchema = sinkOperators[0]->getOutputSchema();
    const OperatorNodePtr sysSourceOperator = createNetworkSourceOperator(currentNesNode, sourceSchema);
    queryPlan->prependPreExistingOperator(sysSourceOperator);
}

// FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void BasePlacementStrategy::addSystemGeneratedOperators(std::string queryId, std::vector<NESTopologyEntryPtr> path) {
    NES_DEBUG("BasePlacementStrategy: Adding system generated operators");

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
            addNetworkSourceOperator(querySubPlan, currentNode, childNesNode);
            NESTopologyEntryPtr parentNesNode = *(pathItr + 1);
            addNetworkSinkOperator(querySubPlan, parentNesNode);

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
                addNetworkSinkOperator(querySubPlan, parentNesNode);
            }

            if (querySubPlan->getSourceOperators().empty()) {
                if (pathItr == path.begin()) {
                    NES_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                              " This should not happen as placement at source node is necessary.");
                    throw QueryPlacementException("BasePlacementStrategy: Unable to find execution node for source node!"
                                                  " This should not happen as placement at source node is necessary.");
                }
                NESTopologyEntryPtr childNesNode = *(pathItr - 1);
                addNetworkSourceOperator(querySubPlan, currentNode, childNesNode);
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
