#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/ExecutionGraph.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/QueryPlacement/BottomUpStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger.hpp>

using namespace std;

namespace NES {

NESExecutionPlanPtr BottomUpStrategy::initializeExecutionPlan(QueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) {

    const QueryPlanPtr queryPlan = inputQuery->getQueryPlan();
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string streamName = inputQuery->getSourceStreamName();

    if (!sourceOperator) {
        NES_ERROR("BottomUp: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodes = StreamCatalog::instance()
                                                        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("BottomUp: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("BottomUp: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("BottomUp: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr> candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("BottomUp: Adding forward operators.");
    addForwardOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("BottomUp: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = sourceOperator->getSourceDescriptor()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr> BottomUpStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                                       const NESTopologyEntryPtr rootNode) const {
    PathFinder pathFinder;
    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource : sourceNodes) {
        vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathBetween(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }
    return candidateNodes;
}

void BottomUpStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                                      LogicalOperatorNodePtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes) {

    TranslateToLegacyPlanPhasePtr translator = TranslateToLegacyPlanPhase::create();
    NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();
    PathFinder pathFinder;

    NES_DEBUG("BottomUp: Place the operator chain from each source node");
    for (NESTopologyEntryPtr sourceNode : sourceNodes) {

        NES_INFO("BottomUp: Find the path between source and sink node");
        const vector<NESTopologyEntryPtr> path = pathFinder.findPathBetween(sourceNode, sinkNode);

        LogicalOperatorNodePtr operatorToPlace = sourceOperator;
        auto pathItr = path.begin();
        NESTopologyEntryPtr nesNodeToPlaceOperator = (*pathItr);

        while (operatorToPlace != nullptr) {

            if (operatorToPlace->instanceOf<SinkLogicalOperatorNode>()) {
                NES_DEBUG("BottomUp: Placing sink node on the sink node");
                nesNodeToPlaceOperator = sinkNode;
            } else if (nesNodeToPlaceOperator->getRemainingCpuCapacity() == 0) {
                NES_DEBUG("BottomUp: Find the next NES node in the path where operator can be placed");
                while (pathItr != path.end()) {
                    ++pathItr;
                    if ((*pathItr)->getRemainingCpuCapacity() > 0) {
                        nesNodeToPlaceOperator = (*pathItr);
                        NES_DEBUG("BottomUp: Found NES node for placing the operators with id : "
                                  + nesNodeToPlaceOperator->getId());
                        break;
                    }
                }
            }

            if ((pathItr == path.end()) || (nesNodeToPlaceOperator->getRemainingCpuCapacity() == 0)) {
                NES_THROW_RUNTIME_ERROR("BottomUp: No resource available for further placement of operators");
            }

            NES_DEBUG("BottomUp: Transforming New Operator into legacy operator");
            OperatorPtr legacyOperator = translator->transform(operatorToPlace);

            if (executionPlanPtr->hasVertex(nesNodeToPlaceOperator->getId())) {

                NES_DEBUG(
                    "BottomUp: node " << nesNodeToPlaceOperator->toString() << " was already used by other deployment");

                const ExecutionNodePtr
                    existingExecutionNode = executionPlanPtr->getExecutionNode(nesNodeToPlaceOperator->getId());

                size_t operatorId = operatorToPlace->getId();

                vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);

                if (exists != residentOperatorIds.end()) {
                    NES_DEBUG("BottomUp: skip adding rest of the operator chains as they already exists.");
                    break;
                } else {

                    NES_DEBUG("BottomUp: Adding the operator the existing execution node");
                    stringstream operatorName;
                    operatorName << existingExecutionNode->getOperatorName() << "=>"
                                 << operatorTypeToString[legacyOperator->getOperatorType()]
                                 << "(OP-" << std::to_string(operatorToPlace->getId()) << ")";
                    existingExecutionNode->setOperatorName(operatorName.str());
                    existingExecutionNode->addOperator(legacyOperator->copy());
                    existingExecutionNode->addOperatorId(operatorToPlace->getId());
                }
            } else {

                NES_DEBUG("BottomUp: create new execution node with id: " << nesNodeToPlaceOperator->getId());
                stringstream operatorName;
                operatorName << operatorTypeToString[legacyOperator->getOperatorType()]
                             << "(OP-" << std::to_string(operatorToPlace->getId()) << ")";
                const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                                to_string(
                                                                                                    nesNodeToPlaceOperator->getId()),
                                                                                                nesNodeToPlaceOperator,
                                                                                                legacyOperator->copy());
                newExecutionNode->addOperatorId(operatorToPlace->getId());
            }

            NES_DEBUG("BottomUp: Reducing the node remaining CPU capacity by 1");
            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            nesNodeToPlaceOperator->reduceCpuCapacity(1);
            if (!operatorToPlace->getParents().empty()) {
                NES_DEBUG("BottomUp: Finding next operator for placement");
                operatorToPlace = operatorToPlace->getParents()[0]->as<LogicalOperatorNode>();
            } else {
                NES_DEBUG("BottomUp: No operator found for placement");
                operatorToPlace = nullptr;
            }
        }
    }
}
}// namespace NES
