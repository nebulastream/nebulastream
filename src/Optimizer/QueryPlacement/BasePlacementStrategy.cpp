#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
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
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>
#include <iostream>

namespace NES {

std::unique_ptr<BasePlacementStrategy> BasePlacementStrategy::getStrategy(NESTopologyPlanPtr nesTopologyPlan, std::string placementStrategyName) {

    if (stringToPlacementStrategyType.find(placementStrategyName) == stringToPlacementStrategyType.end()) {
        throw std::invalid_argument("BasePlacementStrategy: Unknown placement strategy name " + placementStrategyName);
    }

    switch (stringToPlacementStrategyType[placementStrategyName]) {
        case BottomUp: return BottomUpStrategy::create(nesTopologyPlan);
        case TopDown: return TopDownStrategy::create(nesTopologyPlan);
            // FIXME: enable them with issue #755
//        case LowLatency: return LowLatencyStrategy::create(nesTopologyPlan);
//        case HighThroughput: return HighThroughputStrategy::create(nesTopologyPlan);
//        case MinimumResourceConsumption: return MinimumResourceConsumptionStrategy::create(nesTopologyPlan);
//        case MinimumEnergyConsumption: return MinimumEnergyConsumptionStrategy::create(nesTopologyPlan);
//        case HighAvailability: return HighAvailabilityStrategy::create(nesTopologyPlan);
    }
}

BasePlacementStrategy::BasePlacementStrategy(NESTopologyPlanPtr nesTopologyPlan) : nesTopologyPlan(nesTopologyPlan) {
    this->pathFinder = PathFinder::create(nesTopologyPlan);
}

OperatorNodePtr BasePlacementStrategy::createSystemSinkOperator(NESTopologyEntryPtr nesNode) {
    return createSinkLogicalOperatorNode(ZmqSinkDescriptor::create(nesNode->getIp(), zmqDefaultPort));
}

OperatorNodePtr BasePlacementStrategy::createSystemSourceOperator(NESTopologyEntryPtr nesNode, SchemaPtr schema) {
    return createSourceLogicalOperatorNode(ZmqSourceDescriptor::create(schema, nesNode->getIp(), zmqDefaultPort));
}

// FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void BasePlacementStrategy::addSystemGeneratedOperators(std::string queryId, std::vector<NESTopologyEntryPtr> path, GlobalExecutionPlanPtr executionPlan) {
    NES_DEBUG("BasePlacementStrategy: Adding system generated operators");

    auto pathItr = path.begin();
    NESTopologyEntryPtr previousNode;
    while (pathItr != path.end()) {

        NESTopologyEntryPtr currentNode = *pathItr;
        const ExecutionNodePtr executionNode = executionPlan->getExecutionNodeByNodeId(currentNode->getId());
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

            const ExecutionNodePtr childExecutionNode = executionPlan->getExecutionNodeByNodeId(childNesNode->getId());
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
            if (executionNode->createNewQuerySubPlan(queryId, sysSourceOperator)) {
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

            const ExecutionNodePtr childExecutionNode = executionPlan->getExecutionNodeByNodeId(childNesNode->getId());
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
            if (executionNode->createNewQuerySubPlan(queryId, sysSourceOperator)) {
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
                if (pathItr == path.end()) {
                    NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find sink operator at the sink node!"
                                            " This should not happen as placement at sink node is necessary.");
                }

                NESTopologyEntryPtr parentNesNode = *(pathItr + 1);
                const OperatorNodePtr sysSinkOperator = createSystemSinkOperator(parentNesNode);

                querySubPlan->appendPreExistingOperator(sysSinkOperator);
                if (executionNode->updateQuerySubPlan(queryId, querySubPlan)) {
                    NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated sink operator.");
                }
            }

            if (querySubPlan->getSourceOperators().empty()) {
                if (pathItr == path.begin()) {
                    NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to find execution node for source node!"
                                            " This should not happen as placement at source node is necessary.");
                }

                NESTopologyEntryPtr childNesNode = *(pathItr - 1);
                const ExecutionNodePtr childExecutionNode = executionPlan->getExecutionNodeByNodeId(childNesNode->getId());
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

                querySubPlan->appendSystemGeneratedOperator(true, sysSourceOperator);
                if (executionNode->updateQuerySubPlan(queryId, querySubPlan)) {
                    NES_THROW_RUNTIME_ERROR("BasePlacementStrategy: Unable to add system generated sink operator.");
                }
            }
        }
        previousNode = (*pathItr);
        ++pathItr;
    }
}

}// namespace NES
