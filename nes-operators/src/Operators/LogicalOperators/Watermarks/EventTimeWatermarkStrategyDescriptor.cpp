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

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Windowing
{

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(NodeFunctionPtr onField, TimeUnit unit)
    : onField(std::move(onField)), unit(std::move(unit))
{
}

WatermarkStrategyDescriptorPtr EventTimeWatermarkStrategyDescriptor::create(const std::shared_ptr<NodeFunction>& onField, TimeUnit unit)
{
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(
        Windowing::EventTimeWatermarkStrategyDescriptor(onField, std::move(unit)));
}

NodeFunctionPtr EventTimeWatermarkStrategyDescriptor::getOnField() const
{
    return onField;
}

void EventTimeWatermarkStrategyDescriptor::setOnField(const NodeFunctionPtr& newField)
{
    this->onField = newField;
}

bool EventTimeWatermarkStrategyDescriptor::equal(WatermarkStrategyDescriptorPtr other)
{
    auto eventTimeWatermarkStrategyDescriptor = NES::Util::as<EventTimeWatermarkStrategyDescriptor>(other);
    return eventTimeWatermarkStrategyDescriptor->onField->equal(onField);
}

TimeUnit EventTimeWatermarkStrategyDescriptor::getTimeUnit() const
{
    return unit;
}

void EventTimeWatermarkStrategyDescriptor::setTimeUnit(const TimeUnit& newUnit)
{
    this->unit = newUnit;
}

std::string EventTimeWatermarkStrategyDescriptor::toString()
{
    std::stringstream ss;
    ss << "TYPE = EVENT-TIME,";
    ss << "FIELD =" << *onField << ",";
    return ss.str();
}

bool EventTimeWatermarkStrategyDescriptor::inferStamp(SchemaPtr schema)
{
    const auto fieldAccessFunction = NES::Util::as<NodeFunctionFieldAccess>(onField);
    auto fieldName = fieldAccessFunction->getFieldName();
    ///Check if the field exists in the schema
    auto existingField = schema->getFieldByName(fieldName);
    if (existingField)
    {
        fieldAccessFunction->updateFieldName(existingField.value()->getName());
        return true;
    }
    else if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
    {
        return true;
    }
    throw FieldNotFound("EventTimeWaterMark is using a non existing field {}", fieldName);
}

}
