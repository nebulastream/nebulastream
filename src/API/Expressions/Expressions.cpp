
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <DataTypes/DataTypeFactory.hpp>

#include <utility>
namespace NES {

ExpressionItem::ExpressionItem(int8_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint8_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int16_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint16_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int32_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint32_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt32(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int64_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint64_t value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(float value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createFloat(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(double value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createDouble(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(bool value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createBoolean(), std::to_string(value))) {}
// TODO Fix;
ExpressionItem::ExpressionItem(const char* value) : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUndefined(), "")) {}

ExpressionItem::ExpressionItem(ValueTypePtr value) : ExpressionItem(ConstantValueExpressionNode::create(std::move(value))) {}

ExpressionItem::ExpressionItem(ExpressionNodePtr exp) : expression(std::move(exp)) {}

FieldAssignmentExpressionNodePtr ExpressionItem::operator=(ExpressionItem assignItem) {
    return operator=(assignItem.getExpressionNode());
}

FieldAssignmentExpressionNodePtr ExpressionItem::operator=(ExpressionNodePtr assignExpression) {
    if (expression->instanceOf<FieldAccessExpressionNode>()) {
        return FieldAssignmentExpressionNode::create(expression->as<FieldAccessExpressionNode>(), assignExpression);
    }
    NES_FATAL_ERROR("Expression API: we can only assign something to a field access expression");
    throw IllegalArgumentException("Expression API: we can only assign something to a field access expression");
}

ExpressionItem Attribute(std::string fieldName) {
    return ExpressionItem(FieldAccessExpressionNode::create(std::move(fieldName)));
}

ExpressionItem Attribute(std::string fieldName, BasicType type) {
    return ExpressionItem(FieldAccessExpressionNode::create(DataTypeFactory::createType(type), std::move(fieldName)));
}

ExpressionNodePtr ExpressionItem::getExpressionNode() {
    return expression;
}

}// namespace NES