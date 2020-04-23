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

    // check if both stamps are equal
    // todo add type implicit casting if we want to support combinations of ints and floats.
    if (!left->getStamp()->isEqual(right->getStamp())) {
        NES_THROW_RUNTIME_ERROR("ArithmeticalExpressioNode: Error during stamp inference. Type of Left was " +
            left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    // as we assume that both stamps are equal we pick the left stamp as a common stamp
    auto commonStamp = left->getStamp();
    // check if the common stamp is numeric and valid for arithmetical expressions
    // todo replace by type check if this is supported by the type system at some point
    auto basicDataType = std::dynamic_pointer_cast<BasicDataType>(commonStamp);
    if (!basicDataType->isUndefined()) {
        auto basicType = basicDataType->getType();
        if (basicType == BasicType::UINT8 || basicType == BasicType::UINT16 || basicType == BasicType::UINT32
            || basicType == BasicType::UINT64 || basicType == BasicType::INT8 || basicType == BasicType::INT16
            || basicType == BasicType::INT32 || basicType == BasicType::INT64 || basicType == BasicType::FLOAT32
            || basicType == BasicType::FLOAT64) {
            stamp = commonStamp;
            NES_DEBUG(
                "ArithmeticalExpressionNode: we assigned the following stamp: " << toString());
            return;
        }
    }
    // the common stamp was not valid.
    stamp = createUndefinedDataType();
    NES_THROW_RUNTIME_ERROR(
        "ArithmeticalExpressionNode: " + commonStamp->toString() + " is not supported by arithmetical expressions");

}

}