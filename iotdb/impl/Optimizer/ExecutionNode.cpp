#include <Operators/Operator.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Topology/NESTopologyEntry.hpp>

namespace NES {

ExecutionNode::ExecutionNode(std::string operatorName,
                             std::string nodeName,
                             NESTopologyEntryPtr nesNode,
                             OperatorPtr rootOperator)
    : operatorName(operatorName),
      nodeName(nodeName),
      nesNode(nesNode),
      rootOperator(rootOperator),
      id(nesNode->getId()) {}

void ExecutionNode::addOperator(OperatorPtr operatorPtr) {
    OperatorPtr root = rootOperator;
    while (root->getParent() != nullptr) {
        root = root->getParent();
    }
    operatorPtr->setChildren({root});
    root->setParent(operatorPtr);
}

void ExecutionNode::addChild(OperatorPtr operatorPtr) {
    OperatorPtr root = rootOperator;
    root->setChildren({operatorPtr});
    operatorPtr->setParent(root);
    rootOperator = operatorPtr;
}

}// namespace NES