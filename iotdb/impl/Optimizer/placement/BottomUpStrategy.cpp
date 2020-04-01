#include <Optimizer/placement/BottomUpStrategy.hpp>
#include <Operators/Operator.hpp>
#include <iostream>
#include <Util/Logger.hpp>
#include <utility>
#include <Optimizer/utils/PathFinder.hpp>

using namespace std;

namespace NES {

NESExecutionPlanPtr BottomUpStrategy::initializeExecutionPlan(InputQueryPtr inputQuery,
                                                              NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->getSourceStream()->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("BottomUp: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const vector<NESTopologyEntryPtr> sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("BottomUp: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }
    setUDFSFromSampleOperatorToSenseSources(inputQuery);

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("BottomUp: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("BottomUp: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr> candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("BottomUp: Adding forward operators.");
    addForwardOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("BottomUp: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr> BottomUpStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                                       const NESTopologyEntryPtr rootNode) const {
    PathFinder pathFinder;
    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource: sourceNodes) {
        vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathBetween(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }
    return candidateNodes;
}

void BottomUpStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                      const NESTopologyGraphPtr nesTopologyGraphPtr,
                                      OperatorPtr sourceOperator,
                                      vector<NESTopologyEntryPtr> sourceNodes) {

    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        //lambda to convert source optr vector to a friendly struct
        deque<ProcessOperator> operatorsToProcess = {ProcessOperator(sourceOperator, nullptr)};

        while (!operatorsToProcess.empty()) {

            ProcessOperator operatorToProcess = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            OperatorPtr targetOperator = operatorToProcess.operatorToProcess;

            NES_DEBUG("BottomUp: try to place operator " << targetOperator->toString())

            // find the node where the operator will be executed
            NESTopologyEntryPtr node = findSuitableNESNodeForOperatorPlacement(
                operatorToProcess, nesTopologyGraphPtr, sourceNode);

            if (node == nullptr) {
                NES_ERROR("BottomUp: Can not schedule the operator. No node found.");
                throw std::runtime_error("Can not schedule the operator. No node found.");
            } else if (node->getRemainingCpuCapacity() <= 0) {
                NES_ERROR(
                    "BottomUp: Can not schedule the operator. No free resource available capacity is="
                        << node->getRemainingCpuCapacity());
                throw std::runtime_error(
                    "Can not schedule the operator. No free resource available.");
            }

            NES_DEBUG("BottomUp: suitable placement for operator " << targetOperator->toString() << " is "
                                                                   << node->toString())

            // If the selected nes node was already used by another operator for placement then do not create a
            // new execution node rather add operator to existing node.
            if (executionPlanPtr->hasVertex(node->getId())) {

                NES_DEBUG("BottomUp: node " << node->toString() << " was already used by other deployment")

                const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                    ->getExecutionNode(node->getId());

                size_t operatorId = targetOperator->getOperatorId();

                vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);

                if (exists != residentOperatorIds.end()) {
                    //skip adding rest of the operator chains as they already exists.
                    NES_DEBUG("BottomUp: skip adding rest of the operator chains as they already exists.")
                    continue;
                } else {

                    stringstream operatorName;
                    operatorName << existingExecutionNode->getOperatorName() << "=>"
                                 << operatorTypeToString[targetOperator->getOperatorType()]
                                 << "(OP-" << std::to_string(targetOperator->getOperatorId()) << ")";
                    existingExecutionNode->setOperatorName(operatorName.str());
                    existingExecutionNode->addOperator(targetOperator->copy());
                    existingExecutionNode->addOperatorId(targetOperator->getOperatorId());

                    if (targetOperator->getParent() != nullptr) {
                        operatorsToProcess.emplace_back(
                            ProcessOperator(targetOperator->getParent(), existingExecutionNode));
                    }
                }
            } else {

                NES_DEBUG("BottomUp: create new execution node " << node->toString())

                stringstream operatorName;
                operatorName << operatorTypeToString[targetOperator->getOperatorType()] << "(OP-"
                             << std::to_string(targetOperator->getOperatorId()) << ")";

                // Create a new execution node
                const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                                to_string(
                                                                                                    node->getId()),
                                                                                                node,
                                                                                                targetOperator->copy());
                newExecutionNode->addOperatorId(targetOperator->getOperatorId());

                if (targetOperator->getParent() != nullptr) {
                    operatorsToProcess.emplace_back(
                        ProcessOperator(targetOperator->getParent(), newExecutionNode));
                }
            }

            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            node->reduceCpuCapacity(1);
        }
    }
}

NESTopologyEntryPtr
BottomUpStrategy::findSuitableNESNodeForOperatorPlacement(const ProcessOperator& operatorToProcess,
                                                          NESTopologyGraphPtr nesTopologyGraphPtr,
                                                          NESTopologyEntryPtr sourceNodePtr) {

    NESTopologyEntryPtr node;

    if (operatorToProcess.operatorToProcess->getOperatorType() == OperatorType::SINK_OP) {
        node = nesTopologyGraphPtr->getRoot();
        NES_DEBUG("BottomUp: place sink on root node " << node->toString())
        return node;
    } else if (operatorToProcess.operatorToProcess->getOperatorType() == OperatorType::SOURCE_OP) {
        NES_DEBUG("BottomUp: place source on source node " << sourceNodePtr->toString())
        return sourceNodePtr;
    } else {
        NESTopologyEntryPtr nesNode = operatorToProcess.parentExecutionNode
            ->getNESNode();
        //if the previous parent node still have capacity. Use it for further operator assignment
        if (nesNode->getRemainingCpuCapacity() > 0) {
            node = nesNode;
            NES_DEBUG("BottomUp: place operator on node with remaining capacity " << node->toString())
        } else {
            NES_DEBUG("BottomUp: place operator on node neighbouring node")

            // else find the neighbouring higher level nodes connected to it
            const vector<NESTopologyLinkPtr>
                allEdgesToNode = nesTopologyGraphPtr->getAllEdgesFromNode(nesNode);

            vector<NESTopologyEntryPtr> neighbouringNodes;

            transform(allEdgesToNode.begin(), allEdgesToNode.end(),
                      back_inserter(neighbouringNodes),
                      [](NESTopologyLinkPtr nesLink) {
                        return nesLink->getDestNode();
                      });

            NESTopologyEntryPtr neighbouringNodeWithMaxCPU = nullptr;

            for (NESTopologyEntryPtr neighbouringNode : neighbouringNodes) {

                if ((neighbouringNodeWithMaxCPU == nullptr)
                    || (neighbouringNode->getRemainingCpuCapacity()
                        > neighbouringNodeWithMaxCPU->getRemainingCpuCapacity())) {

                    neighbouringNodeWithMaxCPU = neighbouringNode;
                }
            }

            if ((neighbouringNodeWithMaxCPU == nullptr)
                or neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() <= 0) {
                node = nullptr;
            } else if (neighbouringNodeWithMaxCPU->getRemainingCpuCapacity() > 0) {
                node = neighbouringNodeWithMaxCPU;
            }
        }
    }

    return node;
};

}
