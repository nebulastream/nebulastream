
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <utility>
namespace NES {
FieldReadExpressionNode::FieldReadExpressionNode(NES::DataTypePtr stamp, std::string fieldName)
    : ExpressionNode(std::move(stamp)), fieldName(std::move(fieldName)) {};

FieldReadExpressionNode::FieldReadExpressionNode(std::string fieldName)
    : FieldReadExpressionNode(nullptr, std::move(fieldName)) {};

}