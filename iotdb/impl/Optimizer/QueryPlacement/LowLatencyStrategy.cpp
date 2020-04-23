#include <Optimizer/placement/LowLatencyStrategy.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Operators/Operator.hpp>

namespace NES {

NESExecutionPlanPtr LowLatencyStrategy::initializeExecutionPlan(NES::InputQueryPtr inputQuery,
                                                                NES::NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("LowLatency: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("LowLatency: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("LowLatency: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("LowLatency: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr> candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("LowLatency: Adding forward operators.");
    addForwardOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("LowLatency: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr> LowLatencyStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<
    NESTopologyEntryPtr>& sourceNodes, const NESTopologyEntryPtr rootNode) const {

    PathFinder pathFinder;
    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource: sourceNodes) {
        //Find the list of nodes connecting the source and destination nodes
        std::vector<NESTopologyEntryPtr>
            nodesOnPath = pathFinder.findPathWithMinLinkLatency(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }

    return candidateNodes;
}

void LowLatencyStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr, NESTopologyGraphPtr nesTopologyGraphPtr,
                                        OperatorPtr sourceOperator, vector<NESTopologyEntryPtr> sourceNodes) {

    PathFinder pathFinder;
    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();
    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        OperatorPtr targetOperator = sourceOperator;
        const vector<NESTopologyEntryPtr> targetPath = pathFinder.findPathWithMinLinkLatency(sourceNode, sinkNode);

        for (NESTopologyEntryPtr node : targetPath) {
            while (node->getRemainingCpuCapacity() > 0 && targetOperator) {

                if (targetOperator->getOperatorType() == SINK_OP) {
                    node = sinkNode;
                }

                if (!executionPlanPtr->hasVertex(node->getId())) {
                    NES_DEBUG("LowLatency: Create new execution node.")
                    stringstream operatorName;
                    operatorName << operatorTypeToString[targetOperator->getOperatorType()] << "(OP-"
                                 << std::to_string(targetOperator->getOperatorId()) << ")";
                    const ExecutionNodePtr newExecutionNode =
                        executionPlanPtr->createExecutionNode(operatorName.str(), to_string(node->getId()), node,
                                                              targetOperator->copy());
                    newExecutionNode->addOperatorId(targetOperator->getOperatorId());
                } else {

                    const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                        ->getExecutionNode(node->getId());
                    size_t operatorId = targetOperator->getOperatorId();
                    vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                    const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);
                    if (exists != residentOperatorIds.end()) {
                        //skip adding rest of the operator chains as they already exists.
                        NES_DEBUG("LowLatency: skip adding rest of the operator chains as they already exists.");
                        targetOperator = nullptr;
                        break;
                    } else {

                        NES_DEBUG("LowLatency: adding target operator to already existing operator chain.");
                        stringstream operatorName;
                        operatorName << existingExecutionNode->getOperatorName() << "=>"
                                     << operatorTypeToString[targetOperator->getOperatorType()]
                                     << "(OP-" << std::to_string(targetOperator->getOperatorId()) << ")";
                        existingExecutionNode->addOperator(targetOperator->copy());
                        existingExecutionNode->setOperatorName(operatorName.str());
                        existingExecutionNode->addOperatorId(targetOperator->getOperatorId());
                    }
                }

                targetOperator = targetOperator->getParent();
                node->reduceCpuCapacity(1);
            }

            if (!targetOperator) {
                break;
            }
        }
    }
}

}




