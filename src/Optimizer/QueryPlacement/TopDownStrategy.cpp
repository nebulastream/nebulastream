#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

std::unique_ptr<TopDownStrategy> TopDownStrategy::create(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr executionPlan) {
    return std::make_unique<TopDownStrategy>(TopDownStrategy(nesTopologyPlan, executionPlan));
}

TopDownStrategy::TopDownStrategy(NESTopologyPlanPtr nesTopologyPlan, GlobalExecutionPlanPtr globalExecutionPlan)
    : BasePlacementStrategy(nesTopologyPlan, globalExecutionPlan) {}

GlobalExecutionPlanPtr TopDownStrategy::updateGlobalExecutionPlan(QueryPlanPtr queryPlan, StreamCatalogPtr streamCatalog) {

    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    if (!sourceOperator->getSourceDescriptor()->hasStreamName()) {
        NES_THROW_RUNTIME_ERROR("BottomUpStrategy: Source Descriptor need stream name");
    }
    const string streamName = sourceOperator->getSourceDescriptor()->getStreamName();

    const vector<NESTopologyEntryPtr>& sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_THROW_RUNTIME_ERROR("TopDownStrategy: Unable to find the source node to place the operator");
    }

    NES_INFO("TopDownStrategy: Placing operators on the nes topology.");
    const string& queryId = queryPlan->getQueryId();
    placeOperators(queryId, sinkOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyPlan->getRootNode();

    for (NESTopologyEntryPtr targetSource : sourceNodes) {
        NES_DEBUG("TopDownStrategy: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(targetSource, rootNode);
        NES_INFO("TopDownStrategy: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, path);
    }

    return globalExecutionPlan;
}

void TopDownStrategy::placeOperators(std::string queryId, LogicalOperatorNodePtr sinkOperator, vector<NESTopologyEntryPtr> nesSourceNodes) {

    const NESTopologyEntryPtr sinkNode = nesTopologyPlan->getRootNode();

    for (NESTopologyEntryPtr nesSourceNode : nesSourceNodes) {

        LogicalOperatorNodePtr candidateOperator = sinkOperator;
        auto path = pathFinder->findPathBetween(nesSourceNode, sinkNode);
        if (path.empty()) {
            NES_THROW_RUNTIME_ERROR("TopDownStrategy: No path exists between sink and source");
        }

        auto pathItr = path.rbegin();
        NESTopologyEntryPtr candidateNesNode = (*pathItr);

        // Loop till all operators are not placed.
        while (candidateOperator) {

            if (candidateOperator->instanceOf<SourceLogicalOperatorNode>()) {
                NES_DEBUG("TopDownStrategy: Placing source operator on the source node");
                candidateNesNode = nesSourceNode;
            } else if (candidateNesNode->getRemainingCpuCapacity() == 0) {
                NES_DEBUG("TopDownStrategy: Find the next NES node in the path where operator can be placed");
                while (pathItr != path.rend()) {
                    if ((*pathItr)->getRemainingCpuCapacity() > 0) {
                        candidateNesNode = (*pathItr);
                        NES_DEBUG("TopDownStrategy: Found NES node for placing the operators with id : " + candidateNesNode->getId());
                        break;
                    }
                    --pathItr;
                }
            }

            if ((pathItr == path.rend()) || (candidateNesNode->getRemainingCpuCapacity() == 0)) {
                NES_THROW_RUNTIME_ERROR("TopDownStrategy: No node available for further placement of operators");
            }

            if (candidateOperator->instanceOf<SinkLogicalOperatorNode>()) {
                if (!globalExecutionPlan->getExecutionNodeByNodeId(candidateNesNode->getId())) {
                    NES_DEBUG("TopDownStrategy: Creating a new root execution node for placing sink operator");
                    const ExecutionNodePtr rootExecutionNode = ExecutionNode::createExecutionNode(candidateNesNode);
                    globalExecutionPlan->addExecutionNodeAsRoot(rootExecutionNode);
                }
            }

            NES_DEBUG("TopDownStrategy: Checking if execution node for the target worker node already present.");

            if (globalExecutionPlan->checkIfExecutionNodeExists(candidateNesNode->getId())) {

                NES_DEBUG("TopDownStrategy: node " << candidateNesNode->toString() << " was already used by other deployment");
                const ExecutionNodePtr candidateExecutionNode = globalExecutionPlan->getExecutionNodeByNodeId(candidateNesNode->getId());

                if (candidateExecutionNode->hasQuerySubPlan(queryId)) {
                    NES_DEBUG("TopDownStrategy: node " << candidateNesNode->toString() << " already contains a query sub plan with the id" << queryId);
                    if (candidateExecutionNode->checkIfQuerySubPlanContainsOperator(queryId, candidateOperator)) {
                        NES_DEBUG("TopDownStrategy: skip to next upstream operator as the target operator is already placed.");
                        vector<NodePtr> children = candidateOperator->getChildren();
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
                            NES_THROW_RUNTIME_ERROR("TopDownStrategy : unable to find query sub plan with id " + queryId);
                        }
                        querySubPlan->prependPreExistingOperator(candidateOperator->copy());
                        if (!candidateExecutionNode->updateQuerySubPlan(queryId, querySubPlan)) {
                            NES_THROW_RUNTIME_ERROR("TopDownStrategy: failed to add operator" + candidateOperator->toString() + "node for query " + queryId);
                        }
                    }
                } else {
                    NES_DEBUG("TopDownStrategy: Adding the operator to an existing execution node");
                    if (!candidateExecutionNode->createNewQuerySubPlan(queryId, candidateOperator->copy())) {
                        NES_THROW_RUNTIME_ERROR("TopDownStrategy: failed to create a new QuerySubPlan execution node for query " + queryId);
                    }
                }
            } else {

                NES_DEBUG("TopDownStrategy: create new execution node with id: " << candidateNesNode->getId());
                ExecutionNodePtr newExecutionNode = ExecutionNode::createExecutionNode(candidateNesNode, queryId, candidateOperator->copy());
                NES_DEBUG("TopDownStrategy: Adding new execution node with id: " << candidateNesNode->getId());
                if (!globalExecutionPlan->addExecutionNode(newExecutionNode)) {
                    NES_THROW_RUNTIME_ERROR("TopDownStrategy: failed to add execution node for query " + queryId);
                }
            }

            NES_DEBUG("TopDownStrategy: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            candidateNesNode->reduceCpuCapacity(1);
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
