#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

std::unique_ptr<BottomUpStrategy> BottomUpStrategy::create(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr executionPlan) {
    return std::make_unique<BottomUpStrategy>(BottomUpStrategy(nesTopologyPlan, executionPlan));
}

BottomUpStrategy::BottomUpStrategy(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr globalExecutionPlan)
    : BasePlacementStrategy(nesTopologyPlan, globalExecutionPlan) {}

GlobalExecutionPlanPtr BottomUpStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog) {

    const string& queryId = queryPlan->getQueryId();
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string streamName = queryPlan->getSourceStreamName();

    if (!sourceOperator) {
        NES_THROW_RUNTIME_ERROR("BottomUpStrategy: No source operator found in the query plan wih id: " + queryId);
    }

    const vector<NESTopologyEntryPtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
    if (sourceNodes.empty()) {
        NES_THROW_RUNTIME_ERROR("BottomUpStrategy: No source found in the topology for stream " + streamName + "for query with id : " + queryId);
    }

    NES_INFO("BottomUpStrategy: Preparing execution plan for query with id : " << queryId);
    placeOperators(queryId, sourceOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyPlan->getRootNode();

    for (NESTopologyEntryPtr targetSource : sourceNodes) {
        NES_DEBUG("BottomUpStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(targetSource, rootNode);
        NES_INFO("BottomUpStrategy: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, path);
    }

    return globalExecutionPlan;
}

void BottomUpStrategy::placeOperators(std::string queryId, LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes) {

    NESTopologyEntryPtr sinkNode = nesTopologyPlan->getRootNode();

    NES_DEBUG("BottomUpStrategy: Place the operator chain from each source node for query with id : " << queryId);
    for (NESTopologyEntryPtr sourceNode : sourceNodes) {

        NES_INFO("BottomUpStrategy: Find the path between source and sink node for query with id : " << queryId);
        const vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(sourceNode, sinkNode);
        if (path.empty()) {
            NES_THROW_RUNTIME_ERROR("BottomUpStrategy: No path exists between sink and source");
        }

        LogicalOperatorNodePtr operatorToPlace = sourceOperator;
        auto pathItr = path.begin();
        NESTopologyEntryPtr candidateNesNode = (*pathItr);
        while (operatorToPlace) {

            if (operatorToPlace->instanceOf<SinkLogicalOperatorNode>()) {
                NES_DEBUG("BottomUpStrategy: Placing sink operator on the sink node");
                candidateNesNode = sinkNode;
                if (!globalExecutionPlan->getExecutionNodeByNodeId(candidateNesNode->getId())) {
                    NES_DEBUG("BottomUpStrategy: Creating a new root execution node for placing sink operator");
                    const ExecutionNodePtr rootExecutionNode = ExecutionNode::createExecutionNode(candidateNesNode);
                    globalExecutionPlan->addExecutionNodeAsRoot(rootExecutionNode);
                }
            } else if (candidateNesNode->getRemainingCpuCapacity() == 0) {
                NES_DEBUG("BottomUpStrategy: Find the next NES node in the path where operator can be placed");
                while (pathItr != path.end()) {
                    ++pathItr;
                    if ((*pathItr)->getRemainingCpuCapacity() > 0) {
                        candidateNesNode = (*pathItr);
                        NES_DEBUG("BottomUpStrategy: Found NES node for placing the operators with id : " << candidateNesNode->getId());
                        break;
                    }
                }
            }

            if ((pathItr == path.end()) || (candidateNesNode->getRemainingCpuCapacity() == 0)) {
                NES_THROW_RUNTIME_ERROR("BottomUpStrategy: No node available for further placement of operators");
            }

            NES_DEBUG("BottomUpStrategy: Checking if execution node for the target worker node already present.");

            if (globalExecutionPlan->checkIfExecutionNodeExists(candidateNesNode->getId())) {

                NES_DEBUG("BottomUpStrategy: node " << candidateNesNode->toString() << " was already used by other deployment");
                const ExecutionNodePtr candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateNesNode->getId());

                if (candidateExecutionNode->hasQuerySubPlan(queryId)) {
                    NES_DEBUG("BottomUpStrategy: node " << candidateNesNode->toString() << " already contains a query sub plan with the id" << queryId);
                    if (candidateExecutionNode->checkIfQuerySubPlanContainsOperator(queryId, operatorToPlace)) {
                        NES_DEBUG("BottomUpStrategy: skip adding rest of the operator chains as they already exists.");
                        break;
                    } else {
                        NES_DEBUG("BottomUpStrategy: Adding the operator to an existing query sub plan on the Execution node");
                        if (!candidateExecutionNode->appendOperatorToQuerySubPlan(queryId, operatorToPlace->copy())) {
                            NES_THROW_RUNTIME_ERROR("BottomUpStrategy: failed to add operator" + operatorToPlace->toString() + "node for query " + queryId);
                        }
                    }
                } else {
                    NES_DEBUG("BottomUpStrategy: Adding the operator to an existing execution node");
                    if (!candidateExecutionNode->createNewQuerySubPlan(queryId, operatorToPlace->copy())) {
                        NES_THROW_RUNTIME_ERROR("BottomUpStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                    }
                }
            } else {

                NES_DEBUG("BottomUpStrategy: create new execution node with id: " << candidateNesNode->getId());
                ExecutionNodePtr newExecutionNode = ExecutionNode::createExecutionNode(candidateNesNode, queryId, operatorToPlace->copy());
                NES_DEBUG("BottomUpStrategy: Adding new execution node with id: " << candidateNesNode->getId());
                if (!globalExecutionPlan->addExecutionNode(newExecutionNode)) {
                    NES_THROW_RUNTIME_ERROR("BottomUpStrategy: failed to add execution node for query " + queryId);
                }
            }

            NES_DEBUG("BottomUpStrategy: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            candidateNesNode->reduceCpuCapacity(1);
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
