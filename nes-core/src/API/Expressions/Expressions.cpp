/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Nodes/Expressions/FieldRenameExpressionNode.hpp>

#include <utility>
namespace NES {

ExpressionItem::ExpressionItem(int8_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt8(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint8_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt8(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int16_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint16_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt16(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int32_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt32(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint32_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt32(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(int64_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(uint64_t value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createUInt64(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(float value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createFloat(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(double value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createDouble(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(bool value)
    : ExpressionItem(DataTypeFactory::createBasicValue(DataTypeFactory::createBoolean(), std::to_string(value))) {}

ExpressionItem::ExpressionItem(const char* value) : ExpressionItem(DataTypeFactory::createFixedCharValue(value)) {}

ExpressionItem::ExpressionItem(std::string const& value) : ExpressionItem(DataTypeFactory::createFixedCharValue(value.c_str())) {}

ExpressionItem::ExpressionItem(ValueTypePtr value) : ExpressionItem(ConstantValueExpressionNode::create(std::move(value))) {}

ExpressionItem::ExpressionItem(ExpressionNodePtr exp) : expression(std::move(exp)) {}

ExpressionItem ExpressionItem::as(std::string newName) {
    //rename expression node
    if (!expression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Renaming is only allowed on Field Access Attributes");
        NES_NOT_IMPLEMENTED();
    }
    auto fieldAccessExpression = expression->as<FieldAccessExpressionNode>();
    return FieldRenameExpressionNode::create(fieldAccessExpression, std::move(newName));
}

FieldAssignmentExpressionNodePtr ExpressionItem::operator=(ExpressionItem assignItem) {
    return operator=(assignItem.getExpressionNode());
}

FieldAssignmentExpressionNodePtr ExpressionItem::operator=(ExpressionNodePtr assignExpression) {
    if (expression->instanceOf<FieldAccessExpressionNode>()) {
        return FieldAssignmentExpressionNode::create(expression->as<FieldAccessExpressionNode>(), assignExpression);
    }
    NES_FATAL_ERROR("Expression API: we can only assign something to a field access expression");
    throw log4cxx::helpers::IllegalArgumentException("Expression API: we can only assign something to a field access expression");
}

ExpressionItem Attribute(std::string fieldName) {
    return ExpressionItem(FieldAccessExpressionNode::create(std::move(fieldName)));
}

ExpressionItem Attribute(std::string fieldName, BasicType type) {
    return ExpressionItem(FieldAccessExpressionNode::create(DataTypeFactory::createType(type), std::move(fieldName)));
}

ExpressionNodePtr ExpressionItem::getExpressionNode() const { return expression; }

}// namespace NES