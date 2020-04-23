#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>
namespace NES {

ArithmeticalExpressionNode::ArithmeticalExpressionNode(DataTypePtr stamp) : BinaryExpressionNode(stamp) {}

/**
 * @brief The current implementation of type inference for arithmetical expressions expects the following conditions:
 * - The operands of an arithmetical expression have to have numerical stamps.
 * - Both operands have to have the exact same stamp -> We currently do not support implicit type casting.
 * If this conditions are true -> the stamp of an arithmetical expression is equal to the type of its left children.
 * @param schema
 */
void ArithmeticalExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // both sub expressions have to be numerical
    if (!left->getStamp()->isNumerical() || !right->getStamp()->isNumerical()) {
        NES_THROW_RUNTIME_ERROR(
            "ArithmeticalExpressionNode: Error during stamp inference. Types need to be Numerical but Left was:" +
                left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    // calculate the common stamp by joining the left and right stamp
    auto commonStamp = left->getStamp()->join(right->getStamp());

    // check if the common stamp is defined
    if (commonStamp->isUndefined()) {
        // the common stamp was not valid.
        stamp = createUndefinedDataType();
        NES_THROW_RUNTIME_ERROR(
            "ArithmeticalExpressionNode: " + commonStamp->toString() + " is not supported by arithmetical expressions");
    }

    stamp = commonStamp;
    NES_DEBUG("ArithmeticalExpressionNode: we assigned the following stamp: " << toString());
}

}