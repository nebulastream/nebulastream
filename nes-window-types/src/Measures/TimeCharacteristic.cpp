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
#include <utility>
#include <Measures/TimeCharacteristic.hpp>
#include <fmt/format.h>


namespace NES::Windowing
{

TimeCharacteristic::TimeCharacteristic(Type type)
    : value(make_expression<FieldAccessExpression>({}, "timestamp")), type(type), unit(TimeUnit(1))
{
}
TimeCharacteristic::TimeCharacteristic(Type type, ExpressionValue value, TimeUnit unit)
    : value(std::move(value)), type(type), unit(std::move(unit))
{
}
std::shared_ptr<TimeCharacteristic> TimeCharacteristic::createEventTime(ExpressionValue field)
{
    return createEventTime(std::move(field), TimeUnit(1));
}

std::shared_ptr<TimeCharacteristic> TimeCharacteristic::createEventTime(ExpressionValue value, const TimeUnit& unit)
{
    return std::make_shared<TimeCharacteristic>(Type::EventTime, std::move(value), unit);
}

std::shared_ptr<TimeCharacteristic> TimeCharacteristic::createIngestionTime()
{
    return std::make_shared<TimeCharacteristic>(Type::IngestionTime);
}

TimeCharacteristic::Type TimeCharacteristic::getType() const
{
    return type;
}
bool TimeCharacteristic::operator==(const TimeCharacteristic& other) const
{
    const auto isFieldsEqual = value == other.value;
    const auto isTypesEqual = this->type == other.type;
    const auto isEqual = isFieldsEqual and isTypesEqual and this->unit == other.unit;
    return isEqual;
}

TimeUnit TimeCharacteristic::getTimeUnit() const
{
    return unit;
}

void TimeCharacteristic::setTimeUnit(const TimeUnit& newUnit)
{
    this->unit = newUnit;
}

std::string TimeCharacteristic::getTypeAsString() const
{
    switch (type)
    {
        case Type::IngestionTime:
            return "IngestionTime";
        case Type::EventTime:
            return "EventTime";
        default:
            return "Unknown TimeCharacteristic type";
    }
}

std::ostream& operator<<(std::ostream& os, const TimeCharacteristic& timeCharacteristic)
{
    return os << fmt::format("TimeCharacteristic(type: {}, field: {})", timeCharacteristic.getTypeAsString(), timeCharacteristic.value);
}
}
