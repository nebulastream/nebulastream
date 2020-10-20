#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/AddExpressionNode.hpp>
#include <utility>
namespace NES {

AddExpressionNode::AddExpressionNode(DataTypePtr stamp) : ArithmeticalExpressionNode(std::move(stamp)){};

AddExpressionNode::AddExpressionNode(AddExpressionNode* other): ArithmeticalExpressionNode(other) {}

ExpressionNodePtr AddExpressionNode::create(const ExpressionNodePtr left, const ExpressionNodePtr right) {
    auto addNode = std::make_shared<AddExpressionNode>(left->getStamp());
    addNode->setChildren(left, right);
    return addNode;
}

bool AddExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<AddExpressionNode>()) {
        auto otherAddNode = rhs->as<AddExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

const std::string AddExpressionNode::toString() const {
    return "AddNode(" + stamp->toString() + ")";
}
ExpressionNodePtr AddExpressionNode::copy() {
    return std::make_shared<AddExpressionNode>(AddExpressionNode(this));
}


}// namespace NES