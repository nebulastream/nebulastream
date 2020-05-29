
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <memory>
#include <utility>
namespace NES {
FieldAssignmentExpressionNode::FieldAssignmentExpressionNode(DataTypePtr stamp)
    : BinaryExpressionNode(std::move(stamp)){};

FieldAssignmentExpressionNodePtr FieldAssignmentExpressionNode::create(FieldAccessExpressionNodePtr fieldAccess,
                                                                       ExpressionNodePtr expressionNodePtr) {
    auto fieldAssignment = std::make_shared<FieldAssignmentExpressionNode>(expressionNodePtr->getStamp());
    fieldAssignment->setChildren(fieldAccess, expressionNodePtr);
    return fieldAssignment;
}

bool FieldAssignmentExpressionNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<FieldAssignmentExpressionNode>()) {
        auto otherFieldAssignment = rhs->as<FieldAssignmentExpressionNode>();
        // a field assignment expression has always two children.
        return getField()->equal(otherFieldAssignment->getField())
            && getAssignment()->equal(otherFieldAssignment->getAssignment());
    }
    return false;
}

const std::string FieldAssignmentExpressionNode::toString() const {
    return "FieldAssignment()";
}

FieldAccessExpressionNodePtr FieldAssignmentExpressionNode::getField() const {
    return getLeft()->as<FieldAccessExpressionNode>();
}

ExpressionNodePtr FieldAssignmentExpressionNode::getAssignment() const {
    return getRight();
}
void FieldAssignmentExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of assignment expression
    getAssignment()->inferStamp(schema);

    // field access
    auto field = getField();
    if (field->getStamp()->isUndefined()) {
        // if the field has no stamp set it to the one of the assignment
        field->setStamp(getAssignment()->getStamp());
    } else {
        // the field already has a type, check if it is compatible with the assignment
        field->getStamp()->isEqual(getAssignment()->getStamp());
    }
}

}// namespace NES