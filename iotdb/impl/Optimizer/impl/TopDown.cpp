#include "Optimizer/impl/TopDown.hpp"

using namespace iotdb;

FogExecutionPlan TopDown::initializeExecutionPlan(InputQueryPtr inputQuery, FogTopologyPlanPtr fogTopologyPlan) {
  FogExecutionPlan executionGraph;
  const OperatorPtr &sinkOperator = inputQuery->getRoot();

  placeOperators(executionGraph, inputQuery, fogTopologyPlan);
  removeNonResidentOperators(executionGraph);

  completeExecutionGraphWithFogTopology(executionGraph, fogTopologyPlan);

  //FIXME: We are assuming that throughout the pipeline the schema would not change.
  Schema &schema = inputQuery->source_stream->getSchema();
  addSystemGeneratedSourceSinkOperators(schema, executionGraph);

  return executionGraph;
}

void TopDown::placeOperators(FogExecutionPlan executionGraph, InputQueryPtr query, FogTopologyPlanPtr fogTopologyPlanPtr) {

  const OperatorPtr &sinkOperator = query->getRoot();
  const FogGraphPtr &fogGraphPtr = fogTopologyPlanPtr->getFogGraph();

  deque<OperatorPtr> operatorsToProcess = {sinkOperator};

  string sourceName = query->source_stream->getName();

  //find the source Node
  vector<FogVertex> sourceNodes;
  const vector<FogVertex> &allVertex = fogGraphPtr->getAllVertex();
  copy_if(allVertex.begin(), allVertex.end(), back_inserter(sourceNodes),
          [sourceName](const FogVertex vertex) {
            if (vertex.ptr->getEntryType() == FogNodeType::Sensor &&
                vertex.ptr->getRemainingCpuCapacity() > 0) {
              FogTopologySensorNodePtr ptr = std::static_pointer_cast<FogTopologySensorNode>(vertex.ptr);
              return ptr->getSensorType() == sourceName;
            }
            return false;
          });

  // FIXME: In the absence of link between source operator and sensor node we will pick first sensor with some capacity
  //  as the source. Refer issue 122.

  if (sourceNodes.empty()) {
    throw "No available source node found in the network to place the operator";
  }

  const FogTopologyEntryPtr &targetSource = sourceNodes[0].ptr;

  // Find the nodes where we can place the operators. First node will be sink and last one will be the target
  // source.
  deque<FogTopologyEntryPtr> candidateNodes = getCandidateFogNodes(fogGraphPtr, targetSource);

  if (candidateNodes.empty()) {
    throw "No path exists between sink and source";
  }

  // Loop till all operators are not placed.
  while (!operatorsToProcess.empty()) {
    OperatorPtr &processOperator = operatorsToProcess.front();
    operatorsToProcess.pop_front();

    if (processOperator->isScheduled()) {
      continue;
    }

    // if operator is of source type then find the sensor node and schedule it there directly.
    if (processOperator->getOperatorType() == OperatorType::SOURCE_OP) {
      FogTopologyEntryPtr sourceNode = targetSource;

      if (executionGraph.hasVertex(targetSource->getId())) {

        const ExecutionNodePtr &executionNode = executionGraph.getExecutionNode(targetSource->getId());
        string oldOperatorName = executionNode->getOperatorName();
        string newName = operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
            + std::to_string(processOperator->operatorId) + ")" + "=>" + oldOperatorName;
        executionNode->setOperatorName(newName);
        executionNode->addChildOperatorId(processOperator->operatorId);
        targetSource->reduceCpuCapacity(1);
      } else {

        if (sourceNode->getRemainingCpuCapacity() <= 0) {
          throw "Unable to schedule source operator";
        }

        string operatorName = operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
            + std::to_string(processOperator->operatorId) + ")";

        executionGraph.createExecutionNode(operatorName, to_string(sourceNode->getId()),
                                           sourceNode, processOperator);
        processOperator->markScheduled(true);
        sourceNode->reduceCpuCapacity(1);
      }
    } else {
      bool scheduled = false;
      for (FogTopologyEntryPtr node : candidateNodes) {
        if (node->getRemainingCpuCapacity() > 0) {
          scheduled = true;

          if (executionGraph.hasVertex(node->getId())) {

            const ExecutionNodePtr &executionNode = executionGraph.getExecutionNode(node->getId());
            string oldOperatorName = executionNode->getOperatorName();
            string newName = operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
                + std::to_string(processOperator->operatorId) + ")" + "=>" + oldOperatorName;
            executionNode->setOperatorName(newName);
            executionNode->addChildOperatorId(processOperator->operatorId);
            node->reduceCpuCapacity(1);

            vector<OperatorPtr> &nextOperatorsToProcess = processOperator->childs;

            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                 back_inserter(operatorsToProcess));

          } else {

            string operatorName = operatorTypeToString[processOperator->getOperatorType()] + "(OP-"
                + std::to_string(processOperator->operatorId) + ")";

            const ExecutionNodePtr &executionNode = executionGraph.createExecutionNode(
                operatorName, to_string(node->getId()),
                node, processOperator);
            processOperator->markScheduled(true);
            node->reduceCpuCapacity(1);

            vector<OperatorPtr> &nextOperatorsToProcess = processOperator->childs;

            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                 back_inserter(operatorsToProcess));
          }
          break;
        }
      }
      if (!scheduled) {
        throw "Unable to schedule operator on the node";
      }
    }
  }

  // Place forward operators
  candidateNodes = getCandidateFogNodes(fogGraphPtr, targetSource);
  while (!candidateNodes.empty()) {
    shared_ptr<FogTopologyEntry> &node = candidateNodes.front();
    candidateNodes.pop_front();
    if (node->getCpuCapacity() == node->getRemainingCpuCapacity()) {
      executionGraph.createExecutionNode("FWD", to_string(node->getId()), node, nullptr);
      node->reduceCpuCapacity(1);
    }
  }

}
