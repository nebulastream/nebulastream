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
#include <API/Expressions/Expressions.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/ExpressionNode.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/WindowAggregations/MedianAggregationDescriptor.hpp>

namespace NES::Windowing {

MedianAggregationDescriptor::MedianAggregationDescriptor(FieldAccessExpressionNodePtr field)
    : WindowAggregationDescriptor(std::move(field)) {
    this->aggregationType = Type::Median;
}
MedianAggregationDescriptor::MedianAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField)) {
    this->aggregationType = Type::Median;
}

WindowAggregationPtr MedianAggregationDescriptor::create(FieldAccessExpressionNodePtr onField,
                                                         FieldAccessExpressionNodePtr asField) {
    return std::make_shared<MedianAggregationDescriptor>(MedianAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MedianAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a  {}", keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<MedianAggregationDescriptor>(MedianAggregationDescriptor(fieldAccess));
}

void MedianAggregationDescriptor::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext,
                                             SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(typeInferencePhaseContext, schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("MedianAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }
    //Set fully qualified name for the as Field
    auto onFieldName = onField->as<FieldAccessExpressionNode>()->getFieldName();
    auto asFieldName = asField->as<FieldAccessExpressionNode>()->getFieldName();

    auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    //If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + asFieldName);
    } else {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(onField->getStamp());
}
WindowAggregationDescriptorPtr MedianAggregationDescriptor::copy() {
    return std::make_shared<MedianAggregationDescriptor>(
        MedianAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

DataTypePtr MedianAggregationDescriptor::getInputStamp() { return onField->getStamp(); }
DataTypePtr MedianAggregationDescriptor::getPartialAggregateStamp() { return DataTypeFactory::createUndefined(); }
DataTypePtr MedianAggregationDescriptor::getFinalAggregateStamp() { return DataTypeFactory::createDouble(); }

}// namespace NES::Windowing