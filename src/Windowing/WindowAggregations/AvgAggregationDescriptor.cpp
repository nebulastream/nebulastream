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
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/AvgAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

AvgAggregationDescriptor::AvgAggregationDescriptor(FieldAccessExpressionNodePtr field)
    : WindowAggregationDescriptor(std::move(field)) {
    this->aggregationType = Avg;
}
AvgAggregationDescriptor::AvgAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField)) {
    this->aggregationType = Avg;
}

WindowAggregationPtr AvgAggregationDescriptor::create(FieldAccessExpressionNodePtr onField,
                                                      FieldAccessExpressionNodePtr asField) {
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr AvgAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(fieldAccess));
}

void AvgAggregationDescriptor::inferStamp(SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("AvgAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    //Set fully qualified name for the as Field
    auto onFieldName = onField->as<FieldAccessExpressionNode>()->getFieldName();
    auto asFieldName = asField->as<FieldAccessExpressionNode>()->getFieldName();
    if (onFieldName != asFieldName) {
        auto identifier = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField->as<FieldAccessExpressionNode>()->updateFieldName(identifier + asFieldName);
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationDescriptorPtr AvgAggregationDescriptor::copy() {
    return std::make_shared<AvgAggregationDescriptor>(AvgAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

DataTypePtr AvgAggregationDescriptor::getInputStamp() { return onField->getStamp(); }
DataTypePtr AvgAggregationDescriptor::getPartialAggregateStamp() { return DataTypeFactory::createUndefined(); }
DataTypePtr AvgAggregationDescriptor::getFinalAggregateStamp() { return DataTypeFactory::createDouble(); }

}// namespace NES::Windowing