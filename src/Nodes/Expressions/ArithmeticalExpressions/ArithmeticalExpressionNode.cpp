#include <Common/DataTypes/DataType.hpp>
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <utility>
namespace NES {

ArithmeticalExpressionNode::ArithmeticalExpressionNode(DataTypePtr stamp) : BinaryExpressionNode(std::move(stamp)) {}
ArithmeticalExpressionNode::ArithmeticalExpressionNode(ArithmeticalExpressionNode* other) : BinaryExpressionNode(other) {}

/**
 * @brief The current implementation of type inference for arithmetical expressions expects that both
 * operands of an arithmetical expression have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void ArithmeticalExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // both sub expressions have to be numerical
    if (!left->getStamp()->isNumeric() || !right->getStamp()->isNumeric()) {
        NES_THROW_RUNTIME_ERROR(
            "ArithmeticalExpressionNode: Error during stamp inference. Types need to be Numerical but Left was:" + left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    // calculate the common stamp by joining the left and right stamp
    auto commonStamp = left->getStamp()->join(right->getStamp());

    // check if the common stamp is defined
    if (commonStamp->isUndefined()) {
        // the common stamp was not valid -> in this case the common stamp is undefined.
        NES_THROW_RUNTIME_ERROR(
            "ArithmeticalExpressionNode: " + commonStamp->toString() + " is not supported by arithmetical expressions");
    }

    stamp = commonStamp;
    NES_DEBUG("ArithmeticalExpressionNode: we assigned the following stamp: " << toString());
}

bool ArithmeticalExpressionNode::equal(NodePtr rhs) const {
    if (rhs->instanceOf<ArithmeticalExpressionNode>()) {
        auto otherAddNode = rhs->as<ArithmeticalExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

const std::string ArithmeticalExpressionNode::toString() const {
    return "ArithmeticalExpression()";
}

}// namespace NES