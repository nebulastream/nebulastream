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
#include <API/Schema.hpp>
#include <Functions/Expression.hpp>
#include <Functions/FieldAccessExpression.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeUnit.hpp>
#include <Operators/LogicalOperators/Watermarks/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Windowing
{

EventTimeWatermarkStrategyDescriptor::EventTimeWatermarkStrategyDescriptor(ExpressionValue onField, TimeUnit unit)
    : onField(std::move(onField)), unit(std::move(unit))
{
}

std::shared_ptr<WatermarkStrategyDescriptor> EventTimeWatermarkStrategyDescriptor::create(ExpressionValue onField, TimeUnit unit)
{
    return std::make_shared<EventTimeWatermarkStrategyDescriptor>(
        Windowing::EventTimeWatermarkStrategyDescriptor(onField, std::move(unit)));
}

ExpressionValue EventTimeWatermarkStrategyDescriptor::getOnField() const
{
    return onField;
}

void EventTimeWatermarkStrategyDescriptor::setOnField(ExpressionValue newField)
{
    this->onField = newField;
}

bool EventTimeWatermarkStrategyDescriptor::equal(std::shared_ptr<WatermarkStrategyDescriptor> other)
{
    const auto eventTimeWatermarkStrategyDescriptor = NES::Util::as<EventTimeWatermarkStrategyDescriptor>(other);
    return eventTimeWatermarkStrategyDescriptor->onField == onField;
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
    ss << "FIELD =" << format_as(onField) << ",";
    return ss.str();
}

bool EventTimeWatermarkStrategyDescriptor::inferStamp(const Schema& schema)
{
    const auto* fieldAccessFunction = onField.as<FieldAccessExpression>();
    INVARIANT(fieldAccessFunction, "Expected fieldAccessFunction, but received a different expression");

    auto fieldName = fieldAccessFunction->getFieldName();
    const auto existingField = schema.getFieldByName(fieldName);
    if (existingField)
    {
        return true;
    }
    else if (fieldName.name == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
    {
        return true;
    }
    throw FieldNotFound("EventTimeWaterMark is using a non existing field {}", fieldName);
}

}
