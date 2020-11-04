#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

SumAggregationDescriptor::SumAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(std::move(field)) {}
SumAggregationDescriptor::SumAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr SumAggregationDescriptor::create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField) {
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr SumAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(fieldAccess));
}

WindowAggregationDescriptor::Type SumAggregationDescriptor::getType() {
    return Sum;
}

void SumAggregationDescriptor::inferStamp(SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("SumAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationDescriptorPtr SumAggregationDescriptor::copy() {
    return std::make_shared<SumAggregationDescriptor>(SumAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

DataTypePtr SumAggregationDescriptor::getInputStamp() {
    return onField->getStamp();
}
DataTypePtr SumAggregationDescriptor::getPartialAggregateStamp() {
    return onField->getStamp();
}
DataTypePtr SumAggregationDescriptor::getFinalAggregateStamp() {
    return onField->getStamp();
}

}// namespace NES::Windowing