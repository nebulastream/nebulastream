
#include <API/ExpressionAPI.hpp>
#include <Nodes/Expressions/BinaryExpressions/AndExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/EqualsExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/GreaterEqualsExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/GreaterExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessEqualsExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/LessExpressionNode.hpp>
#include <Nodes/Expressions/BinaryExpressions/OrExpressionNode.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldReadExpressionNode.hpp>
#include <Nodes/Expressions/UnaryExpressions/NegateExpressionNode.hpp>
#include <utility>
namespace NES {

ExpressionItem::ExpressionItem(int8_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::INT8, std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint8_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::UINT8, std::to_string(value))) {}

ExpressionItem::ExpressionItem(int16_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::INT16, std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint16_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::UINT16, std::to_string(value))) {}

ExpressionItem::ExpressionItem(int32_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::INT32, std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint32_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::UINT32, std::to_string(value))) {}

ExpressionItem::ExpressionItem(int64_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::INT64, std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint64_t value) :
    ExpressionItem(createBasicTypeValue(BasicType::UINT64, std::to_string(value))) {}

ExpressionItem::ExpressionItem(float value) :
    ExpressionItem(createBasicTypeValue(BasicType::FLOAT32, std::to_string(value))) {}

ExpressionItem::ExpressionItem(double value) :
    ExpressionItem(createBasicTypeValue(BasicType::FLOAT64, std::to_string(value))) {}

ExpressionItem::ExpressionItem(bool value) :
    ExpressionItem(createBasicTypeValue(BasicType::BOOLEAN, std::to_string(value))) {}

ExpressionItem::ExpressionItem(const char* value) :
    ExpressionItem(createStringValueType(value)) {}

ExpressionItem::ExpressionItem(ValueTypePtr value):
    ExpressionItem(ConstantValueExpressionNode::create(std::move(value))){}

ExpressionItem::ExpressionItem(ExpressionNodePtr exp):expression(std::move(exp)) {}

ExpressionItem Attribute(std::string fieldName) {
    return ExpressionItem(FieldReadExpressionNode::create(std::move(fieldName)));
}

ExpressionItem Attribute(std::string fieldName, BasicType type) {
    return ExpressionItem(FieldReadExpressionNode::create(createDataType(type), std::move(fieldName)));
}

ExpressionNodePtr ExpressionItem::getExpressionNode() {
    return expression;
}

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return OrExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return AndExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return EqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return NegateExpressionNode::create(EqualsExpressionNode::create(leftExp, rightExp));
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return LessExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterEqualsExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionNodePtr rightExp) {
    return GreaterExpressionNode::create(leftExp, rightExp);
}

ExpressionNodePtr operator!(ExpressionNodePtr exp) {
    return NegateExpressionNode::create(exp);
}

ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() || rightExp;
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() && rightExp;
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() == rightExp;
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() != rightExp;
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() <= rightExp;
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() < rightExp;
}

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() >= rightExp;
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionNodePtr rightExp) {
    return leftExp.getExpressionNode() > rightExp;
}

ExpressionNodePtr operator||(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp < rightExp.getExpressionNode();
}

ExpressionNodePtr operator>=(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionNodePtr leftExp, ExpressionItem rightExp) {
    return leftExp > rightExp.getExpressionNode();
}

ExpressionNodePtr operator||(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() || rightExp.getExpressionNode();
}

ExpressionNodePtr operator&&(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() && rightExp.getExpressionNode();
}

ExpressionNodePtr operator==(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() == rightExp.getExpressionNode();
}

ExpressionNodePtr operator!=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() != rightExp.getExpressionNode();
}

ExpressionNodePtr operator<=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() <= rightExp.getExpressionNode();
}

ExpressionNodePtr operator<(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() < rightExp.getExpressionNode();
}

ExpressionNodePtr operator>=(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() >= rightExp.getExpressionNode();
}

ExpressionNodePtr operator>(ExpressionItem leftExp, ExpressionItem rightExp) {
    return leftExp.getExpressionNode() > rightExp.getExpressionNode();
}

ExpressionNodePtr operator!(ExpressionItem leftExp) {
    return !leftExp.getExpressionNode();
}
}