
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <utility>
namespace NES {
FieldAssignmentExpressionNode::FieldAssignmentExpressionNode(DataTypePtr stamp)
    : BinaryExpressionNode(std::move(stamp)) {};

ExpressionNodePtr FieldAssignmentExpressionNode::create(FieldAccessExpressionNodePtr fieldAccess, ExpressionNodePtr expressionNodePtr) {
    auto fieldAssignment = std::make_shared<FieldAssignmentExpressionNode>(expressionNodePtr->getStamp());
    fieldAssignment->setChildren(fieldAccess, expressionNodePtr);
    return fieldAssignment;
}


bool FieldAssignmentExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldAssignmentExpressionNode>()) {
        auto otherFieldAssignment = rhs->as<FieldAssignmentExpressionNode>();
        // a field assignment expression has always two children.
        return children[0]->equal(otherFieldAssignment->children[0]) && children[1]->equal(otherFieldAssignment->children[1]);
    }
    return false;
}

const std::string FieldAssignmentExpressionNode::toString() const {
    return "FieldAssignment()";
}

}