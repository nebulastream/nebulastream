#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>

namespace NES {

TimeCharacteristic::TimeCharacteristic(Type type) : type(type) {}
TimeCharacteristic::TimeCharacteristic(Type type, AttributeFieldPtr field) : type(type), field(field) {}

TimeCharacteristicPtr TimeCharacteristic::createEventTime(ExpressionItem fieldValue) {
    auto keyExpression = fieldValue.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    AttributeFieldPtr keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<TimeCharacteristic>(Type::EventTime, keyField);
}

TimeCharacteristicPtr TimeCharacteristic::createProcessingTime() {
    return std::make_shared<TimeCharacteristic>(Type::ProcessingTime);
}

AttributeFieldPtr TimeCharacteristic::getField() {
    return field;
}

TimeCharacteristic::Type TimeCharacteristic::getType() {
    return type;
}

}// namespace NES