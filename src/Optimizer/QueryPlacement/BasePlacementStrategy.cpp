#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/QueryPlacement/HighAvailabilityStrategy.hpp>
#include <Optimizer/QueryPlacement/HighThroughputStrategy.hpp>
#include <Optimizer/QueryPlacement/LowLatencyStrategy.hpp>
#include <Optimizer/QueryPlacement/MinimumEnergyConsumptionStrategy.hpp>
#include <Optimizer/QueryPlacement/MinimumResourceConsumptionStrategy.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <SourceSink/SenseSource.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>
#include <exception>
#include <iostream>

namespace NES {

std::unique_ptr<BasePlacementStrategy> BasePlacementStrategy::getStrategy(NESTopologyPlanPtr nesTopologyPlan, std::string placementStrategyName) {

    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        throw std::invalid_argument("BasePlacementStrategy: Unknown placement strategy name " + placementStrategyName);
    }

    switch (stringToPlacementStrategyType[placementStrategyName]) {
        case BottomUp: return BottomUpStrategy::create(nesTopologyPlan);
        case TopDown: return TopDownStrategy::create(nesTopologyPlan);
        case LowLatency: return LowLatencyStrategy::create(nesTopologyPlan);
        case HighThroughput: return HighThroughputStrategy::create(nesTopologyPlan);
        case MinimumResourceConsumption: return MinimumResourceConsumptionStrategy::create(nesTopologyPlan);
        case MinimumEnergyConsumption: return MinimumEnergyConsumptionStrategy::create(nesTopologyPlan);
        case HighAvailability: return HighAvailabilityStrategy::create(nesTopologyPlan);
    }
}

BasePlacementStrategy::BasePlacementStrategy(NESTopologyPlanPtr nesTopologyPlan) : nesTopologyPlan(nesTopologyPlan) {
    this->pathFinder = PathFinder::create(nesTopologyPlan);
}

// FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void BasePlacementStrategy::addSystemGeneratedSourceSinkOperators(SchemaPtr schema,
                                                                  NESExecutionPlanPtr nesExecutionPlanPtr) {

    const std::shared_ptr<ExecutionGraph>& exeGraph = nesExecutionPlanPtr->getExecutionGraph();
    const vector<ExecutionVertex>& executionNodes = exeGraph->getAllVertex();
    for (ExecutionVertex executionNode : executionNodes) {
        ExecutionNodePtr executionNodePtr = executionNode.ptr;

        // Convert fwd operator
        if (executionNodePtr->getOperatorName() == "FWD") {
            convertFwdOptr(schema, executionNodePtr);
            continue;
        }

        OperatorPtr rootOperator = executionNodePtr->getRootOperator();
        if (rootOperator == nullptr) {
            continue;
        }

        if (rootOperator->getOperatorType() != SOURCE_OP) {
            // create sys introduced src operator
            // Note: the source zmq is always located at localhost
            BufferManagerPtr bPtr;
            QueryManagerPtr dPtr;
            OperatorPtr
                sysSrcOptr = createSourceOperator(createZmqSource(schema, bPtr, dPtr, "localhost", zmqDefaultPort));

            // bind sys introduced operators to each other
            rootOperator->setChildren({sysSrcOptr});
            sysSrcOptr->setParent(rootOperator);

            // Update the operator name
            string optrName = executionNodePtr->getOperatorName();
            optrName = "SOURCE(SYS)=>" + optrName;
            executionNodePtr->setOperatorName(optrName);

            // change the root operator to point to the sys introduced sink operator
            executionNodePtr->setRootOperator(sysSrcOptr);
        }

        OperatorPtr traverse = rootOperator;
        while (traverse->getParent() != nullptr) {
            traverse = traverse->getParent();
        }

        if (traverse->getOperatorType() != SINK_OP) {

            // create sys introduced sink operator

            const vector<ExecutionEdge>& edges = exeGraph->getAllEdgesFromNode(executionNodePtr);
            // FIXME: More than two sources are not supported feature at this moment. Once the feature is available
            // please
            // fix the source code
            const string& destHostName = edges[0].ptr->getDestination()->getNESNode()->getIp();

            const OperatorPtr sysSinkOptr = createSinkOperator(createZmqSink(schema, destHostName, zmqDefaultPort));

            // Update the operator name
            string optrName = executionNodePtr->getOperatorName();
            optrName = optrName + "=>SINK(SYS)";
            executionNodePtr->setOperatorName(optrName);

            // bind sys introduced operators to each other
            sysSinkOptr->setChildren({traverse});
            traverse->setParent(sysSinkOptr);
        }
    }
}

void BasePlacementStrategy::convertFwdOptr(SchemaPtr schema, ExecutionNodePtr executionNodePtr) const {

    // create sys introduced src and sink operators
    // Note: src operator is using localhost because src zmq will run locally
    BufferManagerPtr bPtr;
    QueryManagerPtr dPtr;
    const OperatorPtr
        sysSrcOptr = createSourceOperator(createZmqSource(schema, bPtr, dPtr, "localhost", zmqDefaultPort));
    const OperatorPtr sysSinkOptr = createSinkOperator(createZmqSink(schema, "localhost", zmqDefaultPort));

    sysSrcOptr->setParent(sysSinkOptr);
    sysSinkOptr->setChildren({sysSrcOptr});

    executionNodePtr->setRootOperator(sysSrcOptr);
    executionNodePtr->setOperatorName("SOURCE(SYS)=>SINK(SYS)");
}

void BasePlacementStrategy::fillExecutionGraphWithTopologyInformation(NESExecutionPlanPtr nesExecutionPlanPtr) {

    NES_DEBUG("BasePlacementStrategy: Filling the execution graph with topology information.");
    const vector<NESTopologyLinkPtr>& allEdges = nesTopologyPlan->getNESTopologyGraph()->getAllEdges();
    NES_DEBUG("BasePlacementStrategy: Get all edges in the Topology and iterate over all edges to identify the nodes"
              " that are not part of the execution graph.");

    NES_DEBUG("BasePlacementStrategy::fillExecutionGraphWithTopologyInformation=" << nesTopologyPlan->getTopologyPlanString());
    for (NESTopologyLinkPtr nesLink : allEdges) {

        size_t srcId = nesLink->getSourceNode()->getId();
        size_t destId = nesLink->getDestNode()->getId();
        size_t linkId = nesLink->getId();
        size_t linkCapacity = nesLink->getLinkCapacity();
        size_t linkLatency = nesLink->getLinkLatency();

        ExecutionNodePtr srcExecutionNode, destExecutionNode;

        NES_DEBUG("BasePlacementStrategy: If sourceNode present in the execution graph then use it, else create "
                  "an empty execution node with no operator.");
        if (nesExecutionPlanPtr->hasVertex(srcId)) {
            srcExecutionNode = nesExecutionPlanPtr->getExecutionNode(srcId);
        } else {
            srcExecutionNode = nesExecutionPlanPtr->createExecutionNode(NO_OPERATOR, to_string(srcId),
                                                                        nesLink->getSourceNode(), nullptr);
        }

        NES_DEBUG("BasePlacementStrategy: If destinationNode present in the execution graph then use it, else create "
                  "an empty execution node with no operator.");
        if (nesExecutionPlanPtr->hasVertex(destId)) {
            destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
        } else {
            destExecutionNode = nesExecutionPlanPtr->createExecutionNode(NO_OPERATOR, to_string(destId),
                                                                         nesLink->getDestNode(), nullptr);
        }

        NES_DEBUG("BasePlacementStrategy: create the execution node link.");
        nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode, linkCapacity,
                                                     linkLatency);
    }
}

OperatorNodePtr BasePlacementStrategy::createSystemSinkOperator(NESTopologyEntryPtr nesNode) {
    return createSinkLogicalOperatorNode(ZmqSinkDescriptor::create(nesNode->getIp(), zmqDefaultPort));
}

OperatorNodePtr BasePlacementStrategy::createSystemSourceOperator(NESTopologyEntryPtr nesNode, SchemaPtr schema) {
    return createSourceLogicalOperatorNode(ZmqSourceDescriptor::create(schema, nesNode->getIp(), zmqDefaultPort));
}


void BasePlacementStrategy::addSystemGeneratedOperators(std::string queryId, std::vector<NESTopologyEntryPtr> path, GlobalExecutionPlanPtr executionPlan) {
    NES_DEBUG("BasePlacementStrategy: Adding system generated operators");

    auto pathItr = path.begin();
    NESTopologyEntryPtr previousNode;
    while (pathItr != path.end()) {

        NESTopologyEntryPtr currentNode = *pathItr;
        const ExecutionNodePtr executionNode = executionPlan->getExecutionNode(currentNode->getId());
        if (!executionNode) {

            if (pathItr == path.begin()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                                        " This should not happen as placement at source node is necessary.");
            }

            if (pathItr == path.end()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find execution node for sink node!"
                                        " This should not happen as placement at sink node is necessary.");
            }

            //create a new execution node for the nes node with forward operators
            const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(currentNode);

            NESTopologyEntryPtr childNesNode = *(pathItr - 1);
            NESTopologyEntryPtr parentNesNode = *(pathItr + 1);

            const ExecutionNodePtr childExecutionNode = executionPlan->getExecutionNode(childNesNode->getId());
            if (!(childExecutionNode)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find child execution node");
            }

            if (!childExecutionNode->querySubPlanExists(queryId)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find query sub plan in child execution node");
            }

            QueryPlanPtr childQuerySubPlan = childExecutionNode->getQuerySubPlan(queryId);
            vector<SinkLogicalOperatorNodePtr> sinkOperators = childQuerySubPlan->getSinkOperators();
            if (sinkOperators.empty()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Found a query sub plan without sink operator");
            }

            SchemaPtr sourceSchema = sinkOperators[0]->getOutputSchema();

            const OperatorNodePtr sysSourceOperator = createSystemSourceOperator(currentNode, sourceSchema);
            if(executionNode->createNewQuerySubPlan(queryId, sysSourceOperator)){
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated source operator.");
            }

            const OperatorNodePtr sysSinkOperator = createSystemSinkOperator(parentNesNode);
            if (executionNode->appendOperatorToQuerySubPlan(queryId, sysSinkOperator)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated sink operator.");
            }

            if (!executionPlan->addExecutionNodeAsParentTo(childExecutionNode->getId(), executionNode)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add execution node with forward operators");
            }

        } else if (!executionNode->querySubPlanExists(queryId)) {
            //create a new query sub plan with forward operators
            if (pathItr == path.begin()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                                        " This should not happen as placement at source node is necessary.");
            }

            if (pathItr == path.end()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find execution node for sink node!"
                                        " This should not happen as placement at sink node is necessary.");
            }

            NESTopologyEntryPtr childNesNode = *(pathItr - 1);
            NESTopologyEntryPtr parentNesNode = *(pathItr + 1);

            const ExecutionNodePtr childExecutionNode = executionPlan->getExecutionNode(childNesNode->getId());
            if (!(childExecutionNode)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find child execution node");
            }

            if (!childExecutionNode->querySubPlanExists(queryId)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find query sub plan in child execution node");
            }

            QueryPlanPtr childQuerySubPlan = childExecutionNode->getQuerySubPlan(queryId);
            vector<SinkLogicalOperatorNodePtr> sinkOperators = childQuerySubPlan->getSinkOperators();
            if (sinkOperators.empty()) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Found a query sub plan without sink operator");
            }

            SchemaPtr sourceSchema = sinkOperators[0]->getOutputSchema();

            //create a new execution node for the nes node with forward operators

            const OperatorNodePtr sysSourceOperator = createSystemSourceOperator(currentNode, sourceSchema);
            if(executionNode->createNewQuerySubPlan(queryId, sysSourceOperator)){
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated source operator.");
            }

            const OperatorNodePtr sysSinkOperator = createSystemSinkOperator(parentNesNode);
            if (executionNode->appendOperatorToQuerySubPlan(queryId, sysSinkOperator)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated sink operator.");
            }

            if (!executionPlan->addExecutionNodeAsParentTo(childExecutionNode->getId(), executionNode)) {
                NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add execution node with forward operators");
            }
        } else {
            const QueryPlanPtr querySubPlan = executionNode->getSubPlan(queryId);
            if (querySubPlan->getSinkOperators().empty()) {
                //add a system generated sink operator
            }

            if (querySubPlan->getSourceOperators().empty()) {
                //add a system generated source operator
            }
        }
        previousNode = (*pathItr);
        ++pathItr;
    }
}

}// namespace NES
