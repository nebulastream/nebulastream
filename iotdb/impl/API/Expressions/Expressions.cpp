
#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

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
    return ExpressionItem(FieldAccessExpressionNode::create(std::move(fieldName)));
}

ExpressionItem Attribute(std::string fieldName, BasicType type) {
    return ExpressionItem(FieldAccessExpressionNode::create(createDataType(type), std::move(fieldName)));
}

ExpressionNodePtr ExpressionItem::getExpressionNode() {
    return expression;
}

}