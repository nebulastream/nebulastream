#include "Optimizer/impl/BottomUp.hpp"
#include <Operators/Operator.hpp>
#include <iostream>
#include <Util/Logger.hpp>
#include <utility>

using namespace NES;
using namespace std;

NESExecutionPlanPtr BottomUp::initializeExecutionPlan(InputQueryPtr inputQuery, NESTopologyPlanPtr nesTopologyPlan) {

    const OperatorPtr sinkOperator = inputQuery->getRoot();

    // FIXME: current implementation assumes that we have only one source stream and therefore only one source operator.
    const string& streamName = inputQuery->source_stream->getName();
    const OperatorPtr sourceOperatorPtr = getSourceOperator(sinkOperator);

    if (!sourceOperatorPtr) {
        NES_ERROR("BottomUp: Unable to find the source operator.");
        throw std::runtime_error("No source operator found in the query plan");
    }

    const deque<NESTopologyEntryPtr> sourceNodePtrs = StreamCatalog::instance()
        .getSourceNodesForLogicalStream(streamName);

    if (sourceNodePtrs.empty()) {
        NES_ERROR("BottomUp: Unable to find the target source: " << streamName);
        throw std::runtime_error("No source found in the topology for stream " + streamName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("BottomUp: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperatorPtr, sourceNodePtrs);

    NES_INFO("BottomUp: Adding forward operators.");
    addForwardOperators(sourceNodePtrs, nesTopologyGraphPtr, nesExecutionPlanPtr);

    NES_INFO("BottomUp: Removing non resident operators from the execution nodes.");
    removeNonResidentOperators(nesExecutionPlanPtr);

    NES_INFO("BottomUp: Generating complete execution Graph.");
    completeExecutionGraphWithNESTopology(nesExecutionPlanPtr, nesTopologyPlan);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    Schema& schema = inputQuery->source_stream->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

void BottomUp::placeOperators(NESExecutionPlanPtr executionPlanPtr, const NESTopologyGraphPtr nesTopologyGraphPtr,
                              OperatorPtr sourceOperator, deque<NESTopologyEntryPtr> sourceNodes) {

    for (NESTopologyEntryPtr sourceNode: sourceNodes) {

        //lambda to convert source optr vector to a friendly struct
        deque<ProcessOperator> operatorsToProcess = {ProcessOperator(sourceOperator, nullptr)};

        while (!operatorsToProcess.empty()) {

            ProcessOperator operatorToProcess = operatorsToProcess.front();
            operatorsToProcess.pop_front();

            OperatorPtr optr = operatorToProcess.operatorToProcess;

            NES_DEBUG("BottomUp: try to place operator " << optr->toString())

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

            NES_DEBUG("BottomUp: suitable placement for operator " << optr->toString() << " is " << node->toString())

            // If the selected nes node was already used by another operator for placement then do not create a
            // new execution node rather add operator to existing node.
            if (executionPlanPtr->hasVertex(node->getId())) {

                NES_DEBUG("BottomUp: node " << node->toString() << " was already used by other deployment")

                const ExecutionNodePtr existingExecutionNode = executionPlanPtr
                    ->getExecutionNode(node->getId());

                size_t operatorId = optr->getOperatorId();

                vector<size_t> &residentOperatorIds = existingExecutionNode->getChildOperatorIds();
                const auto exists = std::find(residentOperatorIds.begin(), residentOperatorIds.end(), operatorId);

                if (exists != residentOperatorIds.end()) {
                    //skip adding rest of the operator chains as they already exists.
                    NES_DEBUG("BottomUp: skip adding rest of the operator chains as they already exists.")
                    continue;
                } else {

                    stringstream operatorName;
                    operatorName << existingExecutionNode->getOperatorName() << "=>"
                                 << operatorTypeToString[optr->getOperatorType()]
                                 << "(OP-" << std::to_string(optr->getOperatorId()) << ")";
                    existingExecutionNode->setOperatorName(operatorName.str());
                    existingExecutionNode->addChildOperatorId(optr->getOperatorId());

                    if (optr->getParent() != nullptr) {
                        operatorsToProcess.emplace_back(
                            ProcessOperator(optr->getParent(), existingExecutionNode));
                    }
                }
            } else {

                NES_DEBUG("BottomUp: create new execution node " << node->toString())

                stringstream operatorName;
                operatorName << operatorTypeToString[optr->getOperatorType()] << "(OP-"
                             << std::to_string(optr->getOperatorId()) << ")";

                // Create a new execution node
                const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                                to_string(node->getId()),
                                                                                                node,
                                                                                                optr->copy());
                newExecutionNode->addChildOperatorId(optr->getOperatorId());

                if (optr->getParent() != nullptr) {
                    operatorsToProcess.emplace_back(
                        ProcessOperator(optr->getParent(), newExecutionNode));
                }
            }

            // Reduce the processing capacity by 1
            // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
            node->reduceCpuCapacity(1);
        }
    }
}

NESTopologyEntryPtr BottomUp::findSuitableNESNodeForOperatorPlacement(const ProcessOperator& operatorToProcess,
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

OperatorPtr BottomUp::getSourceOperator(OperatorPtr root) {

    deque<OperatorPtr> bfsTraverse = {root};

    while (!bfsTraverse.empty()) {

        while (!bfsTraverse.empty()) {
            auto optr = bfsTraverse.front();
            bfsTraverse.pop_front();

            if (optr->getOperatorType() == OperatorType::SOURCE_OP) {
                return optr;
            }

            vector<OperatorPtr> children = optr->getChildren();
            copy(children.begin(), children.end(), back_inserter(bfsTraverse));
        }
    }
    return nullptr;
};

// This method returns all sensor nodes that act as the source in the nes topology.
deque<NESTopologyEntryPtr> BottomUp::getSourceNodes(NESTopologyPlanPtr nesTopologyPlan, std::string streamName) {

    //  assert(0);
    //TODO:should not be called anymore in the future
    const NESTopologyEntryPtr rootNode = nesTopologyPlan->getRootNode();
    deque<NESTopologyEntryPtr> listOfSourceNodes;
    deque<NESTopologyEntryPtr> bfsTraverse;
    bfsTraverse.push_back(rootNode);

    while (!bfsTraverse.empty()) {
        auto& node = bfsTraverse.front();
        bfsTraverse.pop_front();

        if (node->getEntryType() == NESNodeType::Sensor) {

            NESTopologySensorNodePtr ptr = std::static_pointer_cast<
                NESTopologySensorNode>(node);

            if (ptr->getPhysicalStreamName() == streamName) {
                listOfSourceNodes.push_back(node);
            }
        }

        const vector<NESTopologyLinkPtr> edgesToNode =
            nesTopologyPlan->getNESTopologyGraph()->getAllEdgesToNode(node);

        for (NESTopologyLinkPtr edgeToNode : edgesToNode) {
            bfsTraverse.push_back(edgeToNode->getSourceNode());
        }
    }

    return listOfSourceNodes;
}
