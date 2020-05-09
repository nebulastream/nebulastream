#include <API/Query.hpp>
#include "Optimizer/QueryPlacement/HighThroughputStrategy.hpp"
#include <Nodes/Operators/QueryPlan.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Operators/Operator.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Phases/TranslateToLegacyPlanPhase.hpp>
#include <Util/Logger.hpp>

namespace NES {

NESExecutionPlanPtr HighThroughputStrategy::initializeExecutionPlan(QueryPtr inputQuery,
                                                                    NESTopologyPlanPtr nesTopologyPlan) {

    const QueryPlanPtr queryPlan = inputQuery->getQueryPlan();
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    auto logicalSourceDescriptor = sourceOperator->getSourceDescriptor()->as<LogicalStreamSourceDescriptor>();
    const string streamName = logicalSourceDescriptor->getStreamName();

    if (!sourceOperator) {
        NES_ERROR("HighThroughput: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("HighThroughput: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("HighThroughput: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("HighThroughput: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr> candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("HighThroughput: Adding forward operators.");
    addForwardOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("HighThroughput: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = logicalSourceDescriptor->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr> HighThroughputStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<
    NESTopologyEntryPtr>& sourceNodes, const NES::NESTopologyEntryPtr rootNode) const {

    PathFinder pathFinder;
    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource: sourceNodes) {
        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr>
            nodesOnPath = pathFinder.findPathWithMaxBandwidth(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }

    return candidateNodes;
}

void HighThroughputStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                            NESTopologyGraphPtr nesTopologyGraphPtr,
                                            LogicalOperatorNodePtr sourceOperator,
                                            vector<NESTopologyEntryPtr> sourceNodes) {

    TranslateToLegacyPlanPhasePtr translator = TranslateToLegacyPlanPhase::create();
    PathFinder pathFinder;
    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();
    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        LogicalOperatorNodePtr targetOperator = sourceOperator;
        const vector<NESTopologyEntryPtr> targetPath = pathFinder.findPathWithMaxBandwidth(sourceNode, sinkNode);

        for (NESTopologyEntryPtr node : targetPath) {
            while (node->getRemainingCpuCapacity() > 0 && targetOperator) {

                if (targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
                    node = sinkNode;
                }

                NES_DEBUG("TopDown: Transforming New Operator into legacy operator");
                OperatorPtr legacyOperator = translator->transform(targetOperator);

                if (!executionPlanPtr->hasVertex(node->getId())) {
                    NES_DEBUG("HighThroughput: Create new execution node.");
                    stringstream operatorName;
                    operatorName << targetOperator->toString()
                                 << "(OP-" << std::to_string(targetOperator->getId()) << ")";
                    const ExecutionNodePtr newExecutionNode =
                        executionPlanPtr->createExecutionNode(operatorName.str(), to_string(node->getId()), node,
                                                              legacyOperator->copy());
                    newExecutionNode->addOperatorId(targetOperator->getId());
                } else {

                    const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                        ->getExecutionNode(node->getId());
                    size_t operatorId = targetOperator->getId();
                    vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                    const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);
                    if (exists != residentOperatorIds.end()) {
                        //skip adding rest of the operator chains as they already exists.
                        NES_DEBUG("HighThroughput: skip adding rest of the operator chains as they already exists.");
                        targetOperator = nullptr;
                        break;
                    } else {

                        NES_DEBUG("HighThroughput: adding target operator to already existing operator chain.");
                        stringstream operatorName;
                        operatorName << existingExecutionNode->getOperatorName() << "=>"
                                     << targetOperator->toString()
                                     << "(OP-" << std::to_string(targetOperator->getId()) << ")";
                        existingExecutionNode->addOperator(legacyOperator->copy());
                        existingExecutionNode->setOperatorName(operatorName.str());
                        existingExecutionNode->addOperatorId(targetOperator->getId());
                    }
                }

                targetOperator = targetOperator->getParents()[0]->as<LogicalOperatorNode>();
                node->reduceCpuCapacity(1);
            }

            if (!targetOperator) {
                break;
            }
        }
    }
}

}// namespace NES
