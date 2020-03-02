#include <Optimizer/NESPlacementOptimizer.hpp>
#include <iostream>
#include <exception>
#include <Optimizer/impl/BottomUp.hpp>
#include <Optimizer/impl/TopDown.hpp>
#include <Optimizer/impl/LowLatency.hpp>
#include <Operators/Operator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <Optimizer/utils/PathFinder.hpp>
#include <Optimizer/impl/HighAvailability.hpp>
#include <Optimizer/impl/HighThroughput.hpp>
#include <Optimizer/impl/MinimumResourceConsumption.hpp>
#include <Optimizer/impl/MinimumEnergyConsumption.hpp>

namespace NES {

std::shared_ptr<NESPlacementOptimizer> NESPlacementOptimizer::getOptimizer(std::string optimizerName) {

    if (optimizerName == "BottomUp") {
        return std::make_unique<BottomUp>(BottomUp());
    } else if (optimizerName == "TopDown") {
        return std::make_unique<TopDown>(TopDown());
    } else if (optimizerName == "Latency") {
        return std::make_unique<LowLatency>(LowLatency());
    } else if (optimizerName == "HighThroughput") {
        return std::make_unique<HighThroughput>(HighThroughput());
    } else if (optimizerName == "MinimumResourceConsumption") {
        return std::make_unique<MinimumResourceConsumption>(MinimumResourceConsumption());
    } else if (optimizerName == "MinimumEnergyConsumption") {
        return std::make_unique<MinimumEnergyConsumption>(MinimumEnergyConsumption());
    } else if (optimizerName == "HighAvailability") {
        return std::make_unique<HighAvailability>(HighAvailability());
    } else {
        throw std::invalid_argument("NESPlacementOptimizer: Unknown optimizer type: " + optimizerName);
    }
}

void NESPlacementOptimizer::removeNonResidentOperators(NESExecutionPlanPtr nesExecutionPlanPtr) {

    const vector<ExecutionVertex>& executionNodes = nesExecutionPlanPtr->getExecutionGraph()->getAllVertex();
    for (ExecutionVertex executionNode: executionNodes) {
        OperatorPtr rootOperator = executionNode.ptr->getRootOperator();
        vector<size_t> childOperatorIds = executionNode.ptr->getChildOperatorIds();
        invalidateUnscheduledOperators(rootOperator, childOperatorIds);
    }
}

void NESPlacementOptimizer::invalidateUnscheduledOperators(OperatorPtr rootOperator, vector<size_t>& childOperatorIds) {

    if (rootOperator == nullptr) {
        return;
    }

    vector<OperatorPtr> children = rootOperator->getChildren();
    OperatorPtr parent = rootOperator->getParent();

    if (parent != nullptr) {
        if (std::find(childOperatorIds.begin(), childOperatorIds.end(), parent->getOperatorId())
            != childOperatorIds.end()) {
            invalidateUnscheduledOperators(parent, childOperatorIds);
        } else {
            rootOperator->setParent(nullptr);
        }
    }

    for (size_t i = 0; i < children.size(); i++) {
        OperatorPtr child = children[i];
        if (std::find(childOperatorIds.begin(), childOperatorIds.end(), child->getOperatorId())
            != childOperatorIds.end()) {
            invalidateUnscheduledOperators(child, childOperatorIds);
        } else {
            children.erase(children.begin() + i);
        }
    }
}

static const int zmqDefaultPort = 5555;
//FIXME: Currently the system is not designed for multiple children. Therefore, the logic is ignoring the fact
// that there could be more than one child. Once the code generator able to deal with it this logic need to be
// fixed.
void NESPlacementOptimizer::addSystemGeneratedSourceSinkOperators(const Schema& schema,
                                                                  NESExecutionPlanPtr nesExecutionPlanPtr) {

    const std::shared_ptr<ExecutionGraph>& exeGraph = nesExecutionPlanPtr->getExecutionGraph();
    const vector<ExecutionVertex>& executionNodes = exeGraph->getAllVertex();
    for (ExecutionVertex executionNode: executionNodes) {
        ExecutionNodePtr executionNodePtr = executionNode.ptr;

        //Convert fwd operator
        if (executionNodePtr->getOperatorName() == "FWD") {
            convertFwdOptr(schema, executionNodePtr);
            continue;
        }

        OperatorPtr rootOperator = executionNodePtr->getRootOperator();
        if (rootOperator == nullptr) {
            continue;
        }

        if (rootOperator->getOperatorType() != SOURCE_OP) {
            //create sys introduced src operator
            //Note: the source zmq is always located at localhost
            OperatorPtr sysSrcOptr = createSourceOperator(createZmqSource(schema, "localhost", zmqDefaultPort));

            //bind sys introduced operators to each other
            rootOperator->setChildren({sysSrcOptr});
            sysSrcOptr->setParent(rootOperator);

            //Update the operator name
            string optrName = executionNodePtr->getOperatorName();
            optrName = "SOURCE(SYS)=>" + optrName;
            executionNodePtr->setOperatorName(optrName);

            //change the root operator to point to the sys introduced sink operator
            executionNodePtr->setRootOperator(sysSrcOptr);
        }

        OperatorPtr traverse = rootOperator;
        while (traverse->getParent() != nullptr) {
            traverse = traverse->getParent();
        }

        if (traverse->getOperatorType() != SINK_OP) {

            //create sys introduced sink operator

            const vector<ExecutionEdge>& edges = exeGraph->getAllEdgesFromNode(executionNodePtr);
            //FIXME: More than two sources are not supported feature at this moment. Once the feature is available please
            // fix the source code
            const string& destHostName = edges[0].ptr->getDestination()->getNESNode()->getIp();
            const OperatorPtr sysSinkOptr = createSinkOperator(createZmqSink(schema, destHostName, zmqDefaultPort));

            //Update the operator name
            string optrName = executionNodePtr->getOperatorName();
            optrName = optrName + "=>SINK(SYS)";
            executionNodePtr->setOperatorName(optrName);

            //bind sys introduced operators to each other
            sysSinkOptr->setChildren({traverse});
            traverse->setParent(sysSinkOptr);
        }

    }
}

void NESPlacementOptimizer::convertFwdOptr(const Schema& schema, ExecutionNodePtr executionNodePtr) const {

    //create sys introduced src and sink operators
    //Note: src operator is using localhost because src zmq will run locally
    const OperatorPtr sysSrcOptr = createSourceOperator(createZmqSource(schema, "localhost", zmqDefaultPort));
    const OperatorPtr sysSinkOptr = createSinkOperator(createZmqSink(schema, "localhost", zmqDefaultPort));

    sysSrcOptr->setParent(sysSinkOptr);
    sysSinkOptr->setChildren({sysSrcOptr});

    executionNodePtr->setRootOperator(sysSrcOptr);
    executionNodePtr->setOperatorName("SOURCE(SYS)=>SINK(SYS)");
}

void NESPlacementOptimizer::completeExecutionGraphWithNESTopology(NESExecutionPlanPtr nesExecutionPlanPtr,
                                                                  NESTopologyPlanPtr nesTopologyPtr) {

    const vector<NESTopologyLinkPtr>& allEdges = nesTopologyPtr->getNESTopologyGraph()->getAllEdges();

    for (NESTopologyLinkPtr nesLink: allEdges) {

        size_t srcId = nesLink->getSourceNode()->getId();
        size_t destId = nesLink->getDestNode()->getId();
        size_t linkId = nesLink->getId();
        size_t linkCapacity = nesLink->getLinkCapacity();
        size_t linkLatency = nesLink->getLinkLatency();

        if (nesExecutionPlanPtr->hasVertex(srcId)) {
            const ExecutionNodePtr srcExecutionNode = nesExecutionPlanPtr->getExecutionNode(srcId);
            if (nesExecutionPlanPtr->hasVertex(destId)) {
                const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
                nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode,
                                                             destExecutionNode,
                                                             linkCapacity,
                                                             linkLatency);
            } else {
                const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->createExecutionNode("NO-OPERATOR",
                                                                                                    to_string(destId),
                                                                                                    nesLink->getDestNode(),
                                                                                                    nullptr);
                nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode,
                                                             linkCapacity,
                                                             linkLatency);
            }
        } else {

            const ExecutionNodePtr
                srcExecutionNode = nesExecutionPlanPtr->createExecutionNode("NO-OPERATOR", to_string(srcId),
                                                                            nesLink->getSourceNode(),
                                                                            nullptr);
            if (nesExecutionPlanPtr->hasVertex(destId)) {
                const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->getExecutionNode(destId);
                nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode,
                                                             linkCapacity,
                                                             linkLatency);
            } else {
                const ExecutionNodePtr destExecutionNode = nesExecutionPlanPtr->createExecutionNode("NO-OPERATOR",
                                                                                                    to_string(destId),
                                                                                                    nesLink->getDestNode(),
                                                                                                    nullptr);
                nesExecutionPlanPtr->createExecutionNodeLink(linkId, srcExecutionNode, destExecutionNode,
                                                             linkCapacity,
                                                             linkLatency);
            }
        }
    }
};

OperatorPtr NESPlacementOptimizer::getSourceOperator(OperatorPtr root) {

    deque<OperatorPtr> operatorTraversQueue = {root};

    while (!operatorTraversQueue.empty()) {

        while (!operatorTraversQueue.empty()) {
            auto optr = operatorTraversQueue.front();
            operatorTraversQueue.pop_front();

            if (optr->getOperatorType() == OperatorType::SOURCE_OP) {
                return optr;
            }

            if (optr->getChildren().empty()) {
                return nullptr;
            }

            vector<OperatorPtr> children = optr->getChildren();
            copy(children.begin(), children.end(), back_inserter(operatorTraversQueue));
        }
    }
    return nullptr;
};

}
