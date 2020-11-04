#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

CountAggregationDescriptor::CountAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(std::move(field)) {}
CountAggregationDescriptor::CountAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField) : WindowAggregationDescriptor(std::move(field), std::move(asField)) {}

WindowAggregationPtr CountAggregationDescriptor::create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField) {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr CountAggregationDescriptor::on() {
    auto countField = FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "count");
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(countField->as<FieldAccessExpressionNode>()));
}

WindowAggregationDescriptor::Type CountAggregationDescriptor::getType() {
    return Count;
}

void CountAggregationDescriptor::inferStamp(SchemaPtr) {
    // a count aggregation is always on an int 64
    asField->setStamp(DataTypeFactory::createInt64());
}
WindowAggregationPtr CountAggregationDescriptor::copy() {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}
DataTypePtr CountAggregationDescriptor::getInputStamp() {
    return DataTypeFactory::createInt64();
}
DataTypePtr CountAggregationDescriptor::getPartialAggregateStamp() {
    return DataTypeFactory::createInt64();
}
DataTypePtr CountAggregationDescriptor::getFinalAggregateStamp() {
    return DataTypeFactory::createInt64();
}
}// namespace NES::Windowing