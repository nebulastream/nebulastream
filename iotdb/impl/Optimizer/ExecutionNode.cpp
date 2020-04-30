#include <Optimizer/ExecutionNode.hpp>
#include <Nodes/Node.hpp>

namespace NES {

ExecutionNode::ExecutionNode(std::string operatorName,
                             std::string nodeName,
                             NESTopologyEntryPtr nesNode,
                             NodePtr rootOperator)
    : id(nesNode->getId()),
      operatorName(operatorName),
      nodeName(nodeName),
      nesNode(nesNode),
      rootOperator(rootOperator) {}

void ExecutionNode::addOperator(NodePtr operatorPtr) {
    NodePtr root = rootOperator;
    while (root->getParent() != nullptr) {
        root = root->getParents()[0];
    }
    root->addParent(operatorPtr);
}

void ExecutionNode::addChild(NodePtr operatorPtr) {
    rootOperator->addChild(operatorPtr);
}

}