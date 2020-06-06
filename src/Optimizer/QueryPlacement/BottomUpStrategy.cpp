#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Nodes/Phases/TypeInferencePhase.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace std;

namespace NES {

BottomUpStrategy::BottomUpStrategy(NESTopologyPlanPtr nesTopologyPlan) : BasePlacementStrategy(nesTopologyPlan) {}

GlobalExecutionPlanPtr BottomUpStrategy::initializeExecutionPlan(QueryPtr inputQuery, StreamCatalogPtr streamCatalog) {

    const QueryPlanPtr queryPlan = inputQuery->getQueryPlan();
    const string& queryId = queryPlan->getQueryId();
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string streamName = queryPlan->getSourceStreamName();

    if (!sourceOperator) {
        NES_ERROR("BottomUp: Unable to find the source operator for query with id : " << queryId);
        throw std::runtime_error("No source operator found in the query plan wih id: " + queryId);
    }

    const vector<NESTopologyEntryPtr> sourceNodes = streamCatalog->getSourceNodesForLogicalStream(streamName);
    if (sourceNodes.empty()) {
        NES_ERROR("BottomUp: Unable to find the target source: " << streamName << " for query with id : " << queryId);
        throw std::runtime_error("No source found in the topology for stream " + streamName + "for query with id : " << queryId);
    }

    NES_INFO("BottomUp: Preparing execution plan for query with id : " << queryId);
    GlobalExecutionPlanPtr executionPlan = placeOperators(queryId, sourceOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyPlan->getRootNode();

    for (NESTopologyEntryPtr targetSource : sourceNodes) {
        NES_DEBUG("BottomUp: Find the path used for performing the placement based on the strategy type for query with id : " << queryId);
        vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(targetSource, rootNode);
        NES_INFO("BottomUp: Adding system generated operators for query with id : " << queryId);
        addSystemGeneratedOperators(queryId, path, executionPlan);
    }

    return executionPlan;
}

vector<NESTopologyEntryPtr> BottomUpStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                                       const NESTopologyEntryPtr rootNode) const {

    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource : sourceNodes) {
        vector<NESTopologyEntryPtr> nodesOnPath = pathFinder->findPathBetween(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }
    return candidateNodes;
}

GlobalExecutionPlanPtr BottomUpStrategy::placeOperators(std::string queryId, LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes) {

    GlobalExecutionPlanPtr executionPlan = GlobalExecutionPlan::create();
    NESTopologyEntryPtr sinkNode = nesTopologyPlan->getRootNode();

    NES_DEBUG("BottomUp: Place the operator chain from each source node for query with id : " << queryId);
    for (NESTopologyEntryPtr sourceNode : sourceNodes) {

        NES_INFO("BottomUp: Find the path between source and sink node for query with id : " << queryId);
        const vector<NESTopologyEntryPtr> path = pathFinder->findPathBetween(sourceNode, sinkNode);
        LogicalOperatorNodePtr operatorToPlace = sourceOperator;
        auto pathItr = path.begin();
        NESTopologyEntryPtr candidateNesNode = (*pathItr);
        while (operatorToPlace != nullptr) {

            if (operatorToPlace->instanceOf<SinkLogicalOperatorNode>()) {
                NES_DEBUG("BottomUp: Placing sink node on the sink node");
                candidateNesNode = sinkNode;
            } else if (candidateNesNode->getRemainingCpuCapacity() == 0) {
                NES_DEBUG("BottomUp: Find the next NES node in the path where operator can be placed");
                while (pathItr != path.end()) {
                    ++pathItr;
                    if ((*pathItr)->getRemainingCpuCapacity() > 0) {
                        candidateNesNode = (*pathItr);
                        NES_DEBUG("BottomUp: Found NES node for placing the operators with id : " + candidateNesNode->getId());
                        break;
                    }
                }
            }

            if ((pathItr == path.end()) || (candidateNesNode->getRemainingCpuCapacity() == 0)) {
                NES_THROW_RUNTIME_ERROR("BottomUp: No node available for further placement of operators");
            }

            NES_DEBUG("BottomUp: Checking if execution node for the target worker node already present.");

            if (executionPlan->executionNodeExists(candidateNesNode->getId())) {

                NES_DEBUG("BottomUp: node " << candidateNesNode->toString() << " was already used by other deployment");
                const ExecutionNodePtr candidateExecutionNode = executionPlan->getExecutionNode(candidateNesNode->getId());

                if (candidateExecutionNode->querySubPlanExists(queryId)) {
                    NES_DEBUG("BottomUp: node " << candidateNesNode->toString() << " already contains a query sub plan with the id" << queryId);
                    if (candidateExecutionNode->querySubPlanContainsOperator(queryId, operatorToPlace)) {
                        NES_DEBUG("BottomUp: skip adding rest of the operator chains as they already exists.");
                        break;
                    } else {
                        NES_DEBUG("BottomUp: Adding the operator to an existing query sub plan on the Execution node");
                        if (!candidateExecutionNode->appendOperatorToQuerySubPlan(queryId, operatorToPlace)) {
                            NES_THROW_RUNTIME_ERROR("BottomUp: failed to add operator" + operatorToPlace->toString() + "node for query " + queryId);
                        }
                    }
                } else {
                    NES_DEBUG("BottomUp: Adding the operator to an existing execution node");
                    if (!candidateExecutionNode->createNewQuerySubPlan(queryId, operatorToPlace)) {
                        NES_THROW_RUNTIME_ERROR("BottomUp: failed to create a new QuerySubPlan execution node for query " + queryId);
                    }
                }
            } else {

                NES_DEBUG("BottomUp: create new execution node with id: " << candidateNesNode->getId());
                ExecutionNodePtr newExecutionNode = ExecutionNode::createExecutionNode(candidateNesNode, queryId, operatorToPlace);
                NES_DEBUG("BottomUp: Adding new execution node with id: " << candidateNesNode->getId());
                if (!executionPlan->addExecutionNode(newExecutionNode)) {
                    NES_THROW_RUNTIME_ERROR("BottomUp: failed to add execution node for query " + queryId);
                }
            }

            NES_DEBUG("BottomUp: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            candidateNesNode->reduceCpuCapacity(1);
            if (!operatorToPlace->getParents().empty()) {
                NES_DEBUG("BottomUp: Finding next operator for placement");
                operatorToPlace = operatorToPlace->getParents()[0]->as<LogicalOperatorNode>();
            } else {
                NES_DEBUG("BottomUp: No operator found for placement");
                operatorToPlace = nullptr;
            }
        }
    }
}// namespace NES
}// namespace NES
