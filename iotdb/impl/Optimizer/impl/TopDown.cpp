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

void TopDown::placeOperators(NESExecutionPlan executionGraph, const OperatorPtr& sinkOperator,
                             deque<NESTopologyEntryPtr> sourceNodes, const NESTopologyGraphPtr& nesGraphPtr) {

    for (NESTopologyEntryPtr& targetSource : sourceNodes) {

        deque<OperatorPtr> operatorsToProcess = {sinkOperator};

        // Find the nodes where we can place the operators. First node will be sink and last one will be the target
        // source.
        deque<NESTopologyEntryPtr> candidateNodes = getCandidateNESNodes(
            nesGraphPtr, targetSource);

        if (candidateNodes.empty()) {
            throw "No path exists between sink and source";
        }

        // Loop till all operators are not placed.
        while (!operatorsToProcess.empty()) {
            OperatorPtr& processOperator = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            // if operator is of source type then find the sensor node and schedule it there directly.
            if (processOperator->getOperatorType() == OperatorType::SOURCE_OP) {
                NESTopologyEntryPtr sourceNode = targetSource;

                if (executionGraph.hasVertex(targetSource->getId())) {

                    const ExecutionNodePtr& executionNode = executionGraph.getExecutionNode(
                        targetSource->getId());
                    string oldOperatorName = executionNode->getOperatorName();
                    string newName =
                        operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
                            + std::to_string(processOperator->getOperatorId()) + ")" + "=>"
                            + oldOperatorName;
                    executionNode->setOperatorName(newName);
                    executionNode->addChildOperatorId(processOperator->getOperatorId());
                    targetSource->reduceCpuCapacity(1);
                } else {

                    if (sourceNode->getRemainingCpuCapacity() <= 0) {
                        throw std::runtime_error("Unable to schedule source operator");
                    }

                    string operatorName = operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
                        + std::to_string(processOperator->getOperatorId()) + ")";

                    ExecutionNodePtr executionNode = executionGraph.createExecutionNode(operatorName,
                                                                                        to_string(sourceNode->getId()),
                                                                                        sourceNode,
                                                                                        processOperator->copy());
                    executionNode->addChildOperatorId(processOperator->getOperatorId());
                    sourceNode->reduceCpuCapacity(1);
                }
            } else {
                bool scheduled = false;
                for (NESTopologyEntryPtr node : candidateNodes) {

                    string newOperatorName = "(OP-" + std::to_string(processOperator->getOperatorId()) + ")";

                    if (executionGraph.hasVertex(node->getId())) {

                        const ExecutionNodePtr& executionNode = executionGraph
                            .getExecutionNode(node->getId());
                        string oldOperatorName = executionNode->getOperatorName();

                        if (oldOperatorName.find(newOperatorName) != string::npos) {

                            //Skip placement of the operator as already placed.
                            //Load the child operators and proceed further
                            scheduled = true;
                            vector<OperatorPtr>& nextOperatorsToProcess = processOperator
                                ->childs;
                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));
                            break;
                        }
                    }

                    if (node->getRemainingCpuCapacity() > 0) {
                        scheduled = true;

                        if (executionGraph.hasVertex(node->getId())) {

                            const ExecutionNodePtr& executionNode = executionGraph
                                .getExecutionNode(node->getId());
                            string oldOperatorName = executionNode->getOperatorName();

                            string newName = operatorTypeToString[processOperator->getOperatorType()];
                            newName.append(newOperatorName)
                                .append("=>")
                                .append(oldOperatorName);

                            executionNode->setOperatorName(newName);
                            executionNode->addChildOperatorId(processOperator->getOperatorId());
                            node->reduceCpuCapacity(1);

                            vector<OperatorPtr>& nextOperatorsToProcess = processOperator
                                ->childs;

                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));

                        } else {

                            string operatorName = operatorTypeToString[processOperator->getOperatorType()];
                            operatorName.append("(OP-")
                                .append(std::to_string(processOperator->getOperatorId()))
                                .append(")");

                            const ExecutionNodePtr& executionNode = executionGraph
                                .createExecutionNode(operatorName, to_string(node->getId()),
                                                     node, processOperator->copy());
                            executionNode->addChildOperatorId(processOperator->getOperatorId());
                            node->reduceCpuCapacity(1);

                            vector<OperatorPtr>& nextOperatorsToProcess = processOperator
                                ->childs;

                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));
                        }
                        break;
                    }
                }
                if (!scheduled) {
                    throw std::runtime_error("Unable to schedule operator on the node");
                }
            }
        }
    }

}
