#include <Optimizer/QueryPlacement/TopDownStrategy.hpp>
#include <API/Query.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Operators/Operator.hpp>
#include <Util/Logger.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Catalogs/StreamCatalog.hpp>

namespace NES {

NESExecutionPlanPtr TopDownStrategy::initializeExecutionPlan(QueryPtr inputQuery,
                                                             NESTopologyPlanPtr nesTopologyPlanPtr) {
    const QueryPlanPtr queryPlan = inputQuery->getQueryPlan();
    const SinkLogicalOperatorNodePtr sinkOperator = queryPlan->getSinkOperators()[0];
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    //find the source Node
    string streamName = inputQuery->getSourceStream()->getName();
    const vector<NESTopologyEntryPtr>& sourceNodes = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

  if (sourceNodes.empty()) {
    NES_ERROR("Unable to find the source node to place the operator");
    throw std::runtime_error("No available source node found in the network to place the operator");
  }

    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlanPtr->getNESTopologyGraph();

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();

    NES_INFO("TopDown: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, sinkOperator, sourceNodes, nesTopologyGraphPtr);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("TopDown: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr>
        candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("TopDown: Adding forward operators.");
    addForwardOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("TopDown: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr, nesTopologyPlanPtr);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = inputQuery->getSourceStream()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr> TopDownStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                                      const NESTopologyEntryPtr rootNode) const {
    PathFinder pathFinder;
    vector<NESTopologyEntryPtr> candidateNodes;
    for (NESTopologyEntryPtr targetSource: sourceNodes) {
        vector<NESTopologyEntryPtr> nodesOnPath = pathFinder.findPathBetween(targetSource, rootNode);
        candidateNodes.insert(candidateNodes.end(), nodesOnPath.begin(), nodesOnPath.end());
    }
    return candidateNodes;
}

void TopDownStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                     LogicalOperatorNodePtr sinkOperator,
                                     vector<NESTopologyEntryPtr> nesSourceNodes,
                                     NESTopologyGraphPtr nesTopologyGraphPtr) {

    PathFinder pathFinder;

    for (NESTopologyEntryPtr nesSourceNode : nesSourceNodes) {

        deque<LogicalOperatorNodePtr> operatorsToProcess = {sinkOperator};

        // Find the nodes where we can place the operators. First node will be sink and last one will be the target
        // source.
        std::vector<NESTopologyEntryPtr>
            candidateNodes = pathFinder.findPathBetween(nesSourceNode, nesTopologyGraphPtr->getRoot());

        if (candidateNodes.empty()) {
            throw std::runtime_error("No path exists between sink and source");
        }

        // Loop till all operators are not placed.
        while (!operatorsToProcess.empty()) {
            LogicalOperatorNodePtr targetOperator = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            if (targetOperator->instanceOf<SourceLogicalOperatorNode>()) {

                string newOperatorName = "(OP-" + std::to_string(targetOperator->getId()) + ")";

                for (auto node = candidateNodes.rbegin(); node != candidateNodes.rend(); node++) {

                    if (executionPlanPtr->hasVertex(node.operator*()->getId())) {

                        const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                            ->getExecutionNode(node.operator*()->getId());

                        size_t operatorId = targetOperator->getId();

                        vector<size_t>& residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                        const auto
                            exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);

                        if (exists != residentOperatorIds.end()) {

                            //Skip placement of the operator as already placed.
                            //Add child operators for placement
                            vector<NodePtr> nextOperatorsToProcess = targetOperator->getChildren();
                            copy(nextOperatorsToProcess.begin(), nextOperatorsToProcess.end(),
                                 back_inserter(operatorsToProcess));
                            break;
                        }
                    }

                    if (node.operator*()->getRemainingCpuCapacity() > 0) {

                        if (executionPlanPtr->hasVertex(node.operator*()->getId())) {

                            const ExecutionNodePtr
                                executionNode = executionPlanPtr->getExecutionNode(node.operator*()->getId());
                            addOperatorToExistingNode(targetOperator, executionNode);
                        } else {
                            createNewExecutionNode(executionPlanPtr, targetOperator, node.operator*());
                        }

                        // Add child operators for placement
                        vector<NodePtr> nextOperatorsToProcess = targetOperator->getChildren();
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

                if (executionPlanPtr->hasVertex(nesSourceNode->getId())) {

                    const ExecutionNodePtr
                        executionNode = executionPlanPtr->getExecutionNode(nesSourceNode->getId());
                    addOperatorToExistingNode(targetOperator, executionNode);
                } else {
                    createNewExecutionNode(executionPlanPtr, targetOperator, nesSourceNode);
                }
                nesSourceNode->reduceCpuCapacity(1);
            }
        }
    }
}

void TopDownStrategy::createNewExecutionNode(NESExecutionPlanPtr executionPlanPtr, LogicalOperatorNodePtr operatorPtr,
                                             NESTopologyEntryPtr nesNode) const {

    stringstream operatorName;
    operatorName << operatorPtr->toString()
                 << "(OP-" << to_string(operatorPtr->getId()) << ")";
    const ExecutionNodePtr
        executionNode = executionPlanPtr->createExecutionNode(operatorName.str(), to_string(nesNode->getId()),
                                                              nesNode, operatorPtr);
    executionNode->addOperatorId(operatorPtr->getId());
}

void TopDownStrategy::addOperatorToExistingNode(LogicalOperatorNodePtr operatorPtr,
                                                ExecutionNodePtr executionNode) const {

    stringstream operatorName;
    operatorName << operatorPtr->toString()
                 << "(OP-" << to_string(operatorPtr->getId()) << ")" << "=>"
                 << executionNode->getOperatorName();
    executionNode->setOperatorName(operatorName.str());
    executionNode->addChild(operatorPtr);
    executionNode->addOperatorId(operatorPtr->getId());
}

}

