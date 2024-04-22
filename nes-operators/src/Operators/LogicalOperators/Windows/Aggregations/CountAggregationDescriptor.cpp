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

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Aggregations/CountAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

CountAggregationDescriptor::CountAggregationDescriptor(FieldAccessExpressionNodePtr field) : WindowAggregationDescriptor(field) {
    this->aggregationType = Type::Count;
}
CountAggregationDescriptor::CountAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField)
    : WindowAggregationDescriptor(field, asField) {
    this->aggregationType = Type::Count;
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::create(FieldAccessExpressionNodePtr onField,
                                                        FieldAccessExpressionNodePtr asField) {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::on() {
    auto countField = FieldAccessExpressionNode::create("count");
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(countField->as<FieldAccessExpressionNode>()));
}

void CountAggregationDescriptor::inferStamp(SchemaPtr schema) {

    auto attributeNameResolver = schema->getSourceNameQualifier() + Schema::ATTRIBUTE_NAME_SEPARATOR;
    auto asFieldName = asField->as<FieldAccessExpressionNode>()->getFieldName();

    //If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + asFieldName);
    } else {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + fieldName);
    }

    // a count aggregation is always on an uint 64
    onField->setStamp(DataTypeFactory::createUInt64());
    asField->setStamp(onField->getStamp());
}

WindowAggregationDescriptorPtr CountAggregationDescriptor::copy() {
    return std::make_shared<CountAggregationDescriptor>(CountAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}
DataTypePtr CountAggregationDescriptor::getInputStamp() { return DataTypeFactory::createUInt64(); }
DataTypePtr CountAggregationDescriptor::getPartialAggregateStamp() { return DataTypeFactory::createUInt64(); }
DataTypePtr CountAggregationDescriptor::getFinalAggregateStamp() { return DataTypeFactory::createUInt64(); }

}// namespace NES::Windowing