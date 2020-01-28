#include "Optimizer/impl/TopDown.hpp"
#include <Util/Logger.hpp>
using namespace NES;

NESExecutionPlan TopDown::initializeExecutionPlan(
    InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlanPtr) {

    const OperatorPtr& sinkOperator = inputQuery->getRoot();

    //find the source Node
    string streamName = inputQuery->source_stream->getName();
    const deque<NESTopologyEntryPtr>& sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodes.empty()) {
        NES_ERROR("Unable to find the source node to place the operator");
        throw std::runtime_error("No available source node found in the network to place the operator");
    }

    const NESTopologyGraphPtr& nesTopologyGraphPtr = nesTopologyPlanPtr->getNESTopologyGraph();

    NESExecutionPlan nesExecutionPlan;

    //Perform operator placement
    placeOperators(nesExecutionPlan, sinkOperator, sourceNodes, nesTopologyGraphPtr);

    addForwardOperators(sourceNodes, nesTopologyGraphPtr, nesExecutionPlan);

    removeNonResidentOperators(nesExecutionPlan);

    completeExecutionGraphWithNESTopology(nesExecutionPlan, nesTopologyPlanPtr);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->source_stream->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlan);

    return nesExecutionPlan;
}

void TopDown::placeOperators(NESExecutionPlan nesExecutionPlan, const OperatorPtr& sinkOperator,
                             deque<NESTopologyEntryPtr> nesSourceNodes, const NESTopologyGraphPtr& nesTopologyGraphPtr) {

    for (NESTopologyEntryPtr& nesSourceNode : nesSourceNodes) {

        deque<OperatorPtr> operatorsToProcess = {sinkOperator};

        // Find the nodes where we can place the operators. First node will be sink and last one will be the target
        // source.
        deque<NESTopologyEntryPtr> candidateNodes = getCandidateNESNodes(nesTopologyGraphPtr, nesSourceNode);

        if (candidateNodes.empty()) {
            throw std::runtime_error("No path exists between sink and source");
        }

        // Loop till all operators are not placed.
        while (!operatorsToProcess.empty()) {
            OperatorPtr& targetOperator = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            if (targetOperator->getOperatorType() != OperatorType::SOURCE_OP) {

                string newOperatorName = "(OP-" + std::to_string(targetOperator->getOperatorId()) + ")";

                for (NESTopologyEntryPtr node : candidateNodes) {

                    if (nesExecutionPlan.hasVertex(node->getId())) {

                        const ExecutionNodePtr& executionNode = nesExecutionPlan.getExecutionNode(node->getId());

                        //Check if the operator is already placed
                        if (executionNode->getOperatorName().find(newOperatorName) != string::npos) {

                            //Skip placement of the operator as already placed.
                            //Add child operators for placement
                            vector<OperatorPtr>& nextOperatorsToProcess = targetOperator->childs;
                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));
                            break;
                        }
                    }

                    if (node->getRemainingCpuCapacity() > 0) {

                        if (nesExecutionPlan.hasVertex(node->getId())) {

                            const ExecutionNodePtr& executionNode = nesExecutionPlan.getExecutionNode(node->getId());
                            addOperatorToExistingNode(targetOperator, executionNode);
                        } else {
                            createNewExecutionNode(nesExecutionPlan, targetOperator, node);
                        }

                        // Add child operators for placement
                        vector<OperatorPtr>& nextOperatorsToProcess = targetOperator->childs;
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

                if (nesExecutionPlan.hasVertex(nesSourceNode->getId())) {

                    const ExecutionNodePtr& executionNode = nesExecutionPlan.getExecutionNode(nesSourceNode->getId());
                    addOperatorToExistingNode(targetOperator, executionNode);
                } else {
                    createNewExecutionNode(nesExecutionPlan, targetOperator, nesSourceNode);
                }
            }
        }
    }
}

void TopDown::createNewExecutionNode(NESExecutionPlan& executionGraph, OperatorPtr& processOperator,
                                     NESTopologyEntryPtr& nesNode) const {

    string operatorName = operatorTypeToString[processOperator->getOperatorType()];
    operatorName.append("(OP-")
        .append(to_string(processOperator->getOperatorId()))
        .append(")");
    const ExecutionNodePtr
        & executionNode = executionGraph.createExecutionNode(operatorName, to_string(nesNode->getId()),
                                                             nesNode, processOperator->copy());
    executionNode->addChildOperatorId(processOperator->getOperatorId());
    executionNode->getNESNode()->reduceCpuCapacity(1);
}

void TopDown::addOperatorToExistingNode(OperatorPtr& operatorPtr, const ExecutionNodePtr& executionNode) const {

    string oldOperatorName = executionNode->getOperatorName();
    string newName =
        operatorTypeToString[operatorPtr->getOperatorType()] + "(OP-"
            + to_string(operatorPtr->getOperatorId()) + ")" + "=>"
            + oldOperatorName;
    executionNode->setOperatorName(newName);
    executionNode->addChildOperatorId(operatorPtr->getOperatorId());
    executionNode->getNESNode()->reduceCpuCapacity(1);
}
