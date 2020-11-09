/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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