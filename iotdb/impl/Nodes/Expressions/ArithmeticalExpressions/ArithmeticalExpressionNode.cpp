#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalExpressionNode.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>
namespace NES {

ArithmeticalExpressionNode::ArithmeticalExpressionNode(DataTypePtr stamp) : BinaryExpressionNode(stamp) {}

void ArithmeticalExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // check if both stamps are equal
    // todo add type conversion if we want to support combinations of ints and floats.
    if (!left->getStamp()->isEqual(right->getStamp())) {
        NES_THROW_RUNTIME_ERROR("ArithmeticalExpressioNode: Error during stamp inference. Type of Left was " +
            left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    auto commonStamp = left->getStamp();
    // check if common stamp is valid for arithmetical expressions
    // todo replace by type check if this is supported by the type system at some point
    auto basicDataType = std::dynamic_pointer_cast<BasicDataType>(commonStamp);
    if (!basicDataType->isUndefined()) {
        auto basicType = basicDataType->getType();
        if (basicType == BasicType::UINT8 || basicType == BasicType::UINT16 || basicType == BasicType::UINT32
            || basicType == BasicType::UINT64 || basicType == BasicType::INT8 || basicType == BasicType::INT16
            || basicType == BasicType::INT32 || basicType == BasicType::INT64 || basicType == BasicType::FLOAT32
            || basicType == BasicType::FLOAT64) {
            stamp = commonStamp;
            return;
        }
    }
    stamp = createUndefinedDataType();
    NES_THROW_RUNTIME_ERROR(
        "ArithmeticalExpressionNode: " + commonStamp->toString() + " is not supported by arithmetical expressions");

}

}