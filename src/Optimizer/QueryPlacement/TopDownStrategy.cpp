#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

std::unique_ptr<TopDownStrategy> TopDownStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                                         StreamCatalogPtr streamCatalog) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

TopDownStrategy::TopDownStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                 StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool TopDownStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
        NES_ERROR("BottomUpStrategy: Source Descriptor need stream name");
        throw QueryPlacementException("BottomUpStrategy: Source Descriptor need stream name");
    }
    const std::string streamName = sourceOperator->getSourceDescriptor()->getStreamName();

    const std::vector<TopologyNodePtr>& sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("TopDownStrategy: Unable to find the source node to place the operator");
        throw QueryPlacementException("TopDownStrategy: Unable to find the source node to place the operator");
    }

    NES_INFO("TopDownStrategy: Placing operators on the nes topology.");
    const QueryId queryId = queryPlan->getQueryId();
    placeOperators(queryId, sinkOperator, sourceNodes);

    TopologyNodePtr rootNode = topology->getRoot();

    for (TopologyNodePtr targetSource : sourceNodes) {
        NES_DEBUG("TopDownStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        TopologyNodePtr startNode = topology->findPathBetween(targetSource, rootNode).value();
        NES_INFO("TopDownStrategy: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, startNode);
    }

    return true;
}

void TopDownStrategy::placeOperators(QueryId queryId, LogicalOperatorNodePtr sinkOperator, std::vector<TopologyNodePtr> nesSourceNodes) {

    const TopologyNodePtr sinkNode = topology->getRoot();

    for (TopologyNodePtr nesSourceNode : nesSourceNodes) {

        LogicalOperatorNodePtr candidateOperator = sinkOperator;
        auto startNode = topology->findPathBetween(nesSourceNode, sinkNode).value();
        NES_DEBUG("TopDownStrategy: inverting the start of the path");
        startNode = startNode->getAllRootNodes()[0]->as<TopologyNode>();
        if (!startNode) {
            NES_ERROR("TopDownStrategy: No path exists between sink and source");
            throw QueryPlacementException("TopDownStrategy: No path exists between sink and source");
        }

        // Loop till all operators are not placed.
        while (candidateOperator) {

            if (candidateOperator->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("TopDownStrategy: Placing source operator on the source node");
                if (startNode->getId() != nesSourceNode->getId()) {
                    startNode = startNode->getAllLeafNodes()[0]->as<TopologyNode>();
                }
            } else if (startNode->getAvailableResources() == 0) {
                NES_DEBUG("TopDownStrategy: Find the next NES node in the path where operator can be placed");
                while (!startNode->getChildren().empty()) {
                    startNode = startNode->getChildren()[0]->as<TopologyNode>();
                    if (startNode->getAvailableResources() > 0) {
                        NES_DEBUG("TopDownStrategy: Found NES node for placing the operators with id : " + startNode->getId());
                        break;
                    }
                }
            }

            if (startNode->getAvailableResources() == 0) {
                NES_ERROR("TopDownStrategy: No node available for further placement of operators");
                throw QueryPlacementException("TopDownStrategy: No node available for further placement of operators");
            }

            if (candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
                if (!globalExecutionPlan->getExecutionNodeByNodeId(startNode->getId())) {
                    NES_DEBUG("TopDownStrategy: Creating a new root execution node for placing sink operator");
                    const ExecutionNodePtr rootExecutionNode = ExecutionNode::createExecutionNode(startNode);
                    globalExecutionPlan->addExecutionNodeAsRoot(rootExecutionNode);
                }
            }

            NES_DEBUG("TopDownStrategy: Checking if execution node for the target worker node already present.");

            if (globalExecutionPlan->checkIfExecutionNodeExists(startNode->getId())) {

                NES_DEBUG("TopDownStrategy: node " << startNode->toString() << " was already used by other deployment");
                const ExecutionNodePtr candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(startNode->getId());

                if (candidateExecutionNode->hasQuerySubPlan(queryId)) {
                    NES_DEBUG("TopDownStrategy: node " << startNode->toString() << " already contains a query sub plan with the id" << queryId);
                    if (candidateExecutionNode->checkIfQuerySubPlanContainsOperator(queryId, candidateOperator)) {
                        NES_DEBUG("TopDownStrategy: skip to next upstream operator as the target operator is already placed.");
                        std::vector<NodePtr> children = candidateOperator->getChildren();
                        if (!children.empty()) {
                            candidateOperator = children[0]->as<LogicalOperatorNode>();
                        } else {
                            candidateOperator = nullptr;
                        }
                        continue;
                    } else {
                        NES_DEBUG("TopDownStrategy: Adding the operator to an existing query sub plan on the Execution node");

                        QueryPlanPtr querySubPlan = candidateExecutionNode->getQuerySubPlan(queryId);
                        if (!querySubPlan) {
                            NES_ERROR("TopDownStrategy : unable to find query sub plan with id " + queryId);
                            throw QueryPlacementException("TopDownStrategy : unable to find query sub plan with id " + queryId);
                        }
                        querySubPlan->prependPreExistingOperator(candidateOperator->copy());
                        if (!candidateExecutionNode->updateQuerySubPlan(queryId, querySubPlan)) {
                            NES_ERROR("TopDownStrategy: failed to add operator" + candidateOperator->toString() + "node for query " + std::to_string(queryId));
                            throw QueryPlacementException("TopDownStrategy: failed to add operator" + candidateOperator->toString() + "node for query " + std::to_string(queryId));
                        }
                    }
                } else {
                    NES_DEBUG("TopDownStrategy: Adding the operator to an existing execution node");
                    if (!candidateExecutionNode->createNewQuerySubPlan(queryId, candidateOperator->copy())) {
                        NES_ERROR("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                        throw QueryPlacementException("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                    }
                }
            } else {

                NES_DEBUG("TopDownStrategy: create new execution node with id: " << startNode->getId());
                ExecutionNodePtr newExecutionNode = ExecutionNode::createExecutionNode(startNode, queryId, candidateOperator->copy());
                NES_DEBUG("TopDownStrategy: Adding new execution node with id: " << startNode->getId());
                if (!globalExecutionPlan->addExecutionNode(newExecutionNode)) {
                    NES_ERROR("TopDownStrategy: failed to add execution node for query " + queryId);
                    throw QueryPlacementException("TopDownStrategy: failed to add execution node for query " + queryId);
                }
            }

            NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            startNode->reduceResources(1);
            topology->reduceResources(startNode->getId(), 1);
            if (!candidateOperator->getChildren().empty()) {
                //FIXME: currently we are not considering split operators
                NES_DEBUG("TopDownStrategy: Finding next operator for placement");
                candidateOperator = candidateOperator->getChildren()[0]->as<LogicalOperatorNode>();
            } else {
                NES_DEBUG("TopDownStrategy: No operator found for placement");
                candidateOperator = nullptr;
            }
        }
    }
    NES_DEBUG("TopDownStrategy: Finished placing query operators into the global execution plan");
}
}// namespace NES
