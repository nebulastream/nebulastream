#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
namespace NES {
ExpressionNode::ExpressionNode(DataTypePtr stamp) : stamp(stamp) {
}

bool ExpressionNode::isPredicate() {
    return stamp->isBoolean();
}

const DataTypePtr ExpressionNode::getStamp() const {
    return stamp;
}

void ExpressionNode::setStamp(DataTypePtr stamp) {
    this->stamp = stamp;
}

void ExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp on all children nodes
    for (const auto& node : children) {
        node->as<ExpressionNode>()->inferStamp(schema);
    }
}

ExpressionNode::ExpressionNode(ExpressionNode* other): stamp(other->stamp) {
}

}// namespace NES