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

#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Schema.hpp>
#include <Exceptions/InvalidFieldException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(const ExpressionItem& onField,
                                                                           TimeMeasure allowedLateness,
                                                                           TimeUnit unit)
    : onField(onField.getExpressionNode()), unit(std::move(unit)), allowedLateness(std::move(allowedLateness)) {}

WatermarkStrategyDescriptorPtr
EventTimeWatermarkStrategyDescriptor::create(const ExpressionItem& onField, TimeMeasure allowedLateness, TimeUnit unit) {
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(
        Windowing::EventTimeWatermarkStrategyDescriptor(onField.getExpressionNode(),
                                                        std::move(allowedLateness),
                                                        std::move(unit)));
}

ExpressionNodePtr EventTimeWatermarkStrategyDescriptor::getOnField() { return onField; }

TimeMeasure EventTimeWatermarkStrategyDescriptor::getAllowedLateness() { return allowedLateness; }

bool EventTimeWatermarkStrategyDescriptor::equal(WatermarkStrategyDescriptorPtr other) {
    auto eventTimeWatermarkStrategyDescriptor = other->as<EventTimeWatermarkStrategyDescriptor>();
    return eventTimeWatermarkStrategyDescriptor->onField == onField
        && eventTimeWatermarkStrategyDescriptor->allowedLateness.getTime() == allowedLateness.getTime();
}

TimeUnit EventTimeWatermarkStrategyDescriptor::getTimeUnit() { return unit; }

std::string EventTimeWatermarkStrategyDescriptor::toString() {
    std::stringstream ss;
    ss << "TYPE = EVENT-TIME,";
    ss << "FIELD =" << onField << ",";
    ss << "ALLOWED-LATENESS =" << allowedLateness.toString();
    return std::string();
}

bool EventTimeWatermarkStrategyDescriptor::inferStamp(SchemaPtr schema) {
    auto fieldAccessExpression = onField->as<FieldAccessExpressionNode>();
    auto fieldName = fieldAccessExpression->getFieldName();
    //Check if the field exists in the schema
    auto existingField = schema->hasFieldName(fieldName);
    if (existingField) {
        fieldAccessExpression->updateFieldName(existingField->getName());
        return true;
    }
    NES_ERROR("EventTimeWaterMark is using a non existing field " + fieldName);
    throw InvalidFieldException("EventTimeWaterMark is using a non existing field " + fieldName);
}

}// namespace NES::Windowing