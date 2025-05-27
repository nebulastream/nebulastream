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

#include <ostream>
#include <string>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <fmt/format.h>


namespace NES::Windowing
{

TimeCharacteristic::TimeCharacteristic(const Type type) : type(type), unit(TimeUnit{1})
{
}

TimeCharacteristic::TimeCharacteristic(const Type type, Schema::Field field, const TimeUnit& unit)
    : field(std::move(field)), type(type), unit(unit)
{
}
TimeCharacteristic TimeCharacteristic::createEventTime(const FieldAccessLogicalFunction& fieldAccess)
{
    return createEventTime(fieldAccess, TimeUnit(1));
}

TimeCharacteristic TimeCharacteristic::createEventTime(const FieldAccessLogicalFunction& fieldAccess, const TimeUnit& unit)
{
    auto keyField = Schema::Field(fieldAccess.getFieldName(), fieldAccess.getDataType());
    return {Type::EventTime, keyField, unit};
}

TimeCharacteristic TimeCharacteristic::createIngestionTime()
{
    return TimeCharacteristic(Type::IngestionTime);
}

TimeCharacteristic::Type TimeCharacteristic::getType() const
{
    return type;
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
    return os << fmt::format("TimeCharacteristic(type: {}, field: {})", timeCharacteristic.getTypeAsString(), timeCharacteristic.field);
}
}
