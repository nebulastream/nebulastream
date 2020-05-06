
#include <Nodes/Expressions/LogicalExpressions/OrExpressionNode.hpp>
namespace NES {
OrExpressionNode::OrExpressionNode() : LogicalBinaryExpressionNode(){};
ExpressionNodePtr OrExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto orNode = std::make_shared<OrExpressionNode>();
    orNode->setChildren(left, right);
    return orNode;
}

bool OrExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<OrExpressionNode>()) {
        auto otherAndNode = rhs->as<OrExpressionNode>();
        return getLeft()->equal(otherAndNode->getLeft()) && getRight()->equal(otherAndNode->getRight());
    }
    return false;
}
const std::string OrExpressionNode::toString() const {
    return "OrNode(" + stamp->toString() + ")";
}

void OrExpressionNode::inferStamp(SchemaPtr schema) {
    // delegate stamp inference of children
    ExpressionNode::inferStamp(schema);
    // check if children stamp is correct
    if (!getLeft()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("OR Expression Node: the stamp of left child must be boolean, but was: " + getLeft()->getStamp()->toString());
    }
    if (!getRight()->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("OR Expression Node: the stamp of left child must be boolean, but was: " + getRight()->getStamp()->toString());
    }
}

}// namespace NES