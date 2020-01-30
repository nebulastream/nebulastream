#include "Optimizer/impl/TopDown.hpp"
#include <Util/Logger.hpp>
using namespace NES;

NESExecutionPlanPtr TopDown::initializeExecutionPlan(
    InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlanPtr) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    //find the source Node
    string streamName = inputQuery->source_stream->getName();
    const deque<NESTopologyEntryPtr>& sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("Unable to find the source node to place the operator");
        throw std::runtime_error("No available source node found in the network to place the operator");
    }

    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlanPtr->getNESTopologyGraph();

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();

    //Perform operator placement
    placeOperators(nesExecutionPlanPtr, sinkOperator, sourceNodes, nesTopologyGraphPtr);

    addForwardOperators(sourceNodes, nesTopologyGraphPtr, nesExecutionPlanPtr);

    removeNonResidentOperators(nesExecutionPlanPtr);

    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlanPtr);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->source_stream->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void TopDown::placeOperators(NESExecutionPlanPtr nesExecutionPlanPtr, const OperatorPtr sinkOperator,
                             deque<NESTopologyEntryPtr> nesSourceNodes, const NESTopologyGraphPtr nesTopologyGraphPtr) {

    for (NESTopologyEntryPtr nesSourceNode : nesSourceNodes) {

        deque<OperatorPtr> operatorsToProcess = {sinkOperator};

        // Find the nodes where we can place the operators. First node will be sink and last one will be the target
        // source.
        deque<NESTopologyEntryPtr> candidateNodes = getCandidateNESNodes(nesTopologyGraphPtr, nesSourceNode);

        if (candidateNodes.empty()) {
            throw std::runtime_error("No path exists between sink and source");
        }

        // Loop till all operators are not placed.
        while (!operatorsToProcess.empty()) {
            OperatorPtr targetOperator = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            if (targetOperator->getOperatorType() != OperatorType::SOURCE_OP) {

                string newOperatorName = "(OP-" + std::to_string(targetOperator->getOperatorId()) + ")";

                for (NESTopologyEntryPtr node : candidateNodes) {

                    if (nesExecutionPlanPtr->hasVertex(node->getId())) {

                        const ExecutionNodePtr executionNode = nesExecutionPlanPtr->getExecutionNode(node->getId());

                        //Check if the operator is already placed
                        if (executionNode->getOperatorName().find(newOperatorName) != string::npos) {

                            //Skip placement of the operator as already placed.
                            //Add child operators for placement
                            vector<OperatorPtr> nextOperatorsToProcess = targetOperator->getChildren();
                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));
                            break;
                        }
                    }

                    if (node->getRemainingCpuCapacity() > 0) {

                        if (nesExecutionPlanPtr->hasVertex(node->getId())) {

                            const ExecutionNodePtr executionNode = nesExecutionPlanPtr->getExecutionNode(node->getId());
                            addOperatorToExistingNode(targetOperator, executionNode);
                        } else {
                            createNewExecutionNode(nesExecutionPlanPtr, targetOperator, node);
                        }

                        // Add child operators for placement
                        vector<OperatorPtr> nextOperatorsToProcess = targetOperator->getChildren();
                        copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                             back_inserter(operatorsToProcess));
                        break;
                    }
                }

                if (operatorsToProcess.empty()) {
                    throw std::runtime_error("Unable to schedule operator on the node");
                }
            } else {
                // if operator is of source type then find the sensor node and schedule it there directly.

                if (nesSourceNode->getRemainingCpuCapacity() <= 0) {
                    throw std::runtime_error("Unable to schedule source operator" + targetOperator->toString());
                }

                if (nesExecutionPlanPtr->hasVertex(nesSourceNode->getId())) {

                    const ExecutionNodePtr executionNode = nesExecutionPlanPtr->getExecutionNode(nesSourceNode->getId());
                    addOperatorToExistingNode(targetOperator, executionNode);
                } else {
                    createNewExecutionNode(nesExecutionPlanPtr, targetOperator, nesSourceNode);
                }
            }
        }
    }
}

void TopDown::createNewExecutionNode(NESExecutionPlanPtr executionPlanPtr, OperatorPtr processOperator,
                                     NESTopologyEntryPtr nesNode) const {

    string operatorName = operatorTypeToString[processOperator->getOperatorType()];
    operatorName.append("(OP-")
        .append(to_string(processOperator->getOperatorId()))
        .append(")");
    const ExecutionNodePtr
        & executionNode = executionPlanPtr->createExecutionNode(operatorName, to_string(nesNode->getId()),
                                                               nesNode, processOperator->copy());
    executionNode->addChildOperatorId(processOperator->getOperatorId());
    executionNode->getNESNode()->reduceCpuCapacity(1);
}

void TopDown::addOperatorToExistingNode(OperatorPtr operatorPtr, const ExecutionNodePtr executionNode) const {

    string oldOperatorName = executionNode->getOperatorName();
    string newName =
        operatorTypeToString[operatorPtr->getOperatorType()] + "(OP-"
            + to_string(operatorPtr->getOperatorId()) + ")" + "=>"
            + oldOperatorName;
    executionNode->setOperatorName(newName);
    executionNode->addChildOperatorId(operatorPtr->getOperatorId());
    executionNode->getNESNode()->reduceCpuCapacity(1);
}
