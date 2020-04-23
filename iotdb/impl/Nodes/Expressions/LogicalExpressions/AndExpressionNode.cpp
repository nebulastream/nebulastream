
#include <Nodes/Expressions/LogicalExpressions/AndExpressionNode.hpp>
namespace NES {
AndExpressionNode::AndExpressionNode() : LogicalBinaryExpressionNode() {};
ExpressionNodePtr AndExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto andNode = std::make_shared<AndExpressionNode>();
    andNode->setChildren(left, right);
    return andNode;
}

bool AndExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<AndExpressionNode>()) {
        auto otherAndNode = rhs->as<AndExpressionNode>();
        return getLeft()->equal(otherAndNode->getLeft()) &&
            getRight()->equal(otherAndNode->getRight());
    }
    return false;
}
const std::string AndExpressionNode::toString() const {
    return "AndNode("+stamp->toString()+")";
}

void AndExpressionNode::inferStamp(SchemaPtr schema) {
    // delegate stamp inference of children
    ExpressionNode::inferStamp(schema);
    // check if children stamp is correct
    if (!getLeft()->getStamp()->isEqual(createDataType(BOOLEAN))) {
        NES_THROW_RUNTIME_ERROR("AND Expression Node: the stamp of left child must be boolean, but was: " + getLeft()->getStamp()->toString());
    }
    if(!getRight()->getStamp()->isEqual(createDataType(BOOLEAN))){
        NES_THROW_RUNTIME_ERROR("AND Expression Node: the stamp of left child must be boolean, but was: " + getRight()->getStamp()->toString());
    }
}

}