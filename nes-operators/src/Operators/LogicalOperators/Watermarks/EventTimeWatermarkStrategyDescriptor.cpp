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

#include <API/AttributeField.hpp>

#include <API/Schema.hpp>
#include <Operators/Exceptions/InvalidFieldException.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Util/Logger/Logger.hpp>
#include <sstream>
#include <utility>

namespace NES::Windowing {

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(const ExpressionNodePtr& onField,
                                                                           TimeMeasure allowedLateness,
                                                                           TimeUnit unit)
    : onField(onField), unit(std::move(unit)), allowedLateness(std::move(allowedLateness)) {}

WatermarkStrategyDescriptorPtr
EventTimeWatermarkStrategyDescriptor::create(const ExpressionNodePtr& onField, TimeMeasure allowedLateness, TimeUnit unit) {
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(
        Windowing::EventTimeWatermarkStrategyDescriptor(onField, std::move(allowedLateness), std::move(unit)));
}

ExpressionNodePtr EventTimeWatermarkStrategyDescriptor::getOnField() const { return onField; }

void EventTimeWatermarkStrategyDescriptor::setOnField(const ExpressionNodePtr& newField) { this->onField = newField; }

TimeMeasure EventTimeWatermarkStrategyDescriptor::getAllowedLateness() const { return allowedLateness; }

bool EventTimeWatermarkStrategyDescriptor::equal(WatermarkStrategyDescriptorPtr other) {
    auto eventTimeWatermarkStrategyDescriptor = other->as<EventTimeWatermarkStrategyDescriptor>();
    return eventTimeWatermarkStrategyDescriptor->onField->equal(onField)
        && eventTimeWatermarkStrategyDescriptor->allowedLateness.getTime() == allowedLateness.getTime();
}

TimeUnit EventTimeWatermarkStrategyDescriptor::getTimeUnit() const { return unit; }

void EventTimeWatermarkStrategyDescriptor::setTimeUnit(const TimeUnit& newUnit) { this->unit = newUnit; }

std::string EventTimeWatermarkStrategyDescriptor::toString() {
    std::stringstream ss;
    ss << "TYPE = EVENT-TIME,";
    ss << "FIELD =" << onField->toString() << ",";
    ss << "ALLOWED-LATENESS =" << allowedLateness.toString();
    return ss.str();
}

bool EventTimeWatermarkStrategyDescriptor::inferStamp( SchemaPtr schema) {
    auto fieldAccessExpression = onField->as<FieldAccessExpressionNode>();
    auto fieldName = fieldAccessExpression->getFieldName();
    //Check if the field exists in the schema
    auto existingField = schema->hasFieldName(fieldName);
    if (existingField) {
        fieldAccessExpression->updateFieldName(existingField->getName());
        return true;
    } else if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
        return true;
    }
    NES_ERROR("EventTimeWaterMark is using a non existing field  {}", fieldName);
    throw InvalidFieldException("EventTimeWaterMark is using a non existing field " + fieldName);
}

}// namespace NES::Windowing