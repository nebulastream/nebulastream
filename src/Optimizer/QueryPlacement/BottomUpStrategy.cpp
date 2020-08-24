#include <Catalogs/StreamCatalog.hpp>
#include <Exceptions/QueryPlacementException.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Phases/TranslateToLegacyPlanPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

using namespace std;
namespace NES {

std::unique_ptr<BottomUpStrategy> BottomUpStrategy::create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology,
                                                           TypeInferencePhasePtr typeInferencePhase, StreamCatalogPtr streamCatalog) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog));
}

BottomUpStrategy::BottomUpStrategy(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase,
                                   StreamCatalogPtr streamCatalog)
    : BasePlacementStrategy(globalExecutionPlan, topology, typeInferencePhase, streamCatalog) {}

bool BottomUpStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan) {

    const QueryId queryId = queryPlan->getQueryId();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    // TONY: now, we are removing the assumption of single source operator.
    // Create multiple query graph by splitting merge operator.
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
        NES_THROW_RUNTIME_ERROR("BottomUpStrategy: Source Descriptor need stream name: " + queryId);
    }
    const string streamName = sourceOperator->getSourceDescriptor()->getStreamName();

    if (!sourceOperator) {
        NES_ERROR("BottomUpStrategy: No source operator found in the query plan wih id: " + queryId);
        throw QueryPlacementException("BottomUpStrategy: No source operator found in the query plan wih id: " + queryId);
    }

    const std::vector<TopologyNodePtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
    if (sourceNodes.empty()) {
        NES_ERROR("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
        throw QueryPlacementException("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + std::to_string(queryId));
    }

    NES_INFO("BottomUpStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryId, sourceOperator, sourceNodes);

    TopologyNodePtr rootNode = topology->getRoot();

    for (TopologyNodePtr targetSource : sourceNodes) {
        NES_DEBUG("BottomUpStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        TopologyNodePtr startNode = topology->findAllPathBetween(targetSource, rootNode).value();
        NES_INFO("BottomUpStrategy: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, startNode);
    }
    return true;
}

void BottomUpStrategy::placeOperators(QueryId queryId, LogicalOperatorNodePtr sourceOperator, vector<TopologyNodePtr> sourceNodes) {

    TopologyNodePtr sinkNode = topology->getRoot();

    NES_DEBUG("BottomUpStrategy: Place the operator chain from each source node for query with id : " << queryId);
    for (const auto& sourceNode : sourceNodes) {

        NES_INFO("BottomUpStrategy: Find the path between source and sink node for query with id : " << queryId);
        PhysicalNodePtr candidatePhysicalNode = topology->findAllPathBetween(sourceNode, sinkNode);
        if (!candidatePhysicalNode) {
            NES_ERROR("BottomUpStrategy: No path exists between sink and source");
            throw QueryPlacementException("BottomUpStrategy: No path exists between sink and source");
        }

        LogicalOperatorNodePtr operatorToPlace = sourceOperator;
        while (operatorToPlace) {

            if (operatorToPlace->instanceOf<SinkLogicalOperatorNode>()) {
                NES_DEBUG("BottomUpStrategy: Placing sink operator on the sink node");
                if (candidatePhysicalNode->getId() != sinkNode->getId()) {
                    candidatePhysicalNode = candidatePhysicalNode->getAllRootNodes()[0]->as<TopologyNode>();
                }
                if (!globalExecutionPlan->getExecutionNodeByNodeId(candidatePhysicalNode->getId())) {
                    NES_DEBUG("BottomUpStrategy: Creating a new root execution node for placing sink operator");
                    const ExecutionNodePtr rootExecutionNode = ExecutionNode::createExecutionNode(candidatePhysicalNode);
                    globalExecutionPlan->addExecutionNodeAsRoot(rootExecutionNode);
                }
            } else if (candidatePhysicalNode->getAvailableResources() == 0) {
                NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
                while (!candidatePhysicalNode->getParents().empty()) {
                    candidatePhysicalNode = candidatePhysicalNode->getParents()[0]->as<TopologyNode>();
                    if (candidatePhysicalNode->getAvailableResources() > 0) {
                        NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : " << candidatePhysicalNode->getId());
                        break;
                    }
                }
            }

            if (candidatePhysicalNode->getAvailableResources() == 0) {
                NES_ERROR("BottomUpStrategy: No node available for further placement of operators");
                throw QueryPlacementException("BottomUpStrategy: No node available for further placement of operators");
            }

            NES_DEBUG("BottomUpStrategy: Checking if execution node for the target worker node already present.");

            if (globalExecutionPlan->checkIfExecutionNodeExists(candidatePhysicalNode->getId())) {

                NES_DEBUG("BottomUpStrategy: node " << candidatePhysicalNode->toString() << " was already used by other deployment");
                const ExecutionNodePtr candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidatePhysicalNode->getId());

                if (candidateExecutionNode->hasQuerySubPlan(queryId)) {
                    NES_DEBUG("BottomUpStrategy: node " << candidatePhysicalNode->toString() << " already contains a query sub plan with the id" << queryId);
                    if (candidateExecutionNode->checkIfQuerySubPlanContainsOperator(queryId, operatorToPlace)) {
                        NES_DEBUG("BottomUpStrategy: skip adding rest of the operator chains as they already exists.");
                        break;
                    } else {
                        NES_DEBUG("BottomUpStrategy: Adding the operator to an existing query sub plan on the Execution node");
                        if (!candidateExecutionNode->appendOperatorToQuerySubPlan(queryId, operatorToPlace->copy())) {
                            NES_ERROR("BottomUpStrategy: failed to add operator" + operatorToPlace->toString() + "node for query " + std::to_string(queryId));
                            throw QueryPlacementException("BottomUpStrategy: failed to add operator" + operatorToPlace->toString() + "node for query " + std::to_string(queryId));
                        }
                    }
                } else {
                    NES_DEBUG("BottomUpStrategy: Adding the operator to an existing execution node");
                    if (!candidateExecutionNode->createNewQuerySubPlan(queryId, operatorToPlace->copy())) {
                        NES_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                        throw QueryPlacementException("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                    }
                }
            } else {

                NES_DEBUG("BottomUpStrategy: create new execution node with id: " << candidatePhysicalNode->getId());
                ExecutionNodePtr newExecutionNode = ExecutionNode::createExecutionNode(candidatePhysicalNode, queryId, operatorToPlace->copy());
                NES_DEBUG("BottomUpStrategy: Adding new execution node with id: " << candidatePhysicalNode->getId());
                if (!globalExecutionPlan->addExecutionNode(newExecutionNode)) {
                    NES_ERROR("BottomUpStrategy: failed to add execution node for query " + queryId);
                    throw QueryPlacementException("BottomUpStrategy: failed to add execution node for query " + queryId);
                }
            }

            NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            candidatePhysicalNode->reduceResources(1);
            topology->reduceResources(candidatePhysicalNode->getId(), 1);
            if (!operatorToPlace->getParents().empty()) {
                //FIXME: currently we are not considering split operators
                NES_DEBUG("BottomUpStrategy: Finding next operator for placement");
                operatorToPlace = operatorToPlace->getParents()[0]->as<LogicalOperatorNode>();
            } else {
                NES_DEBUG("BottomUpStrategy: No operator found for placement");
                operatorToPlace = nullptr;
            }
        }
    }
    NES_DEBUG("BottomUpStrategy: Finished placing query operators into the global execution plan");
}
}// namespace NES
