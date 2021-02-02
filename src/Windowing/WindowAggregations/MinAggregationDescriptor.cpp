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
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

MinAggregationDescriptor::MinAggregationDescriptor(FieldAccessExpressionNodePtr field)
    : WindowAggregationDescriptor(std::move(field)) {
    this->aggregationType = Min;
}
MinAggregationDescriptor::MinAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField)) {
    this->aggregationType = Min;
}

WindowAggregationPtr MinAggregationDescriptor::create(FieldAccessExpressionNodePtr onField,
                                                      FieldAccessExpressionNodePtr asField) {
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MinAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(fieldAccess));
}

DataTypePtr MinAggregationDescriptor::getInputStamp() { return onField->getStamp(); }
DataTypePtr MinAggregationDescriptor::getPartialAggregateStamp() { return onField->getStamp(); }
DataTypePtr MinAggregationDescriptor::getFinalAggregateStamp() { return onField->getStamp(); }

void MinAggregationDescriptor::inferStamp(SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("SumAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationPtr MinAggregationDescriptor::copy() {
    return std::make_shared<MinAggregationDescriptor>(MinAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

}// namespace NES::Windowing