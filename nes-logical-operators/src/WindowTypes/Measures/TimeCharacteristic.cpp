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
#include <API/AttributeField.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>


namespace NES::Windowing
{

TimeCharacteristic::TimeCharacteristic(Type type) : type(type), unit(TimeUnit(1))
{
}

TimeCharacteristic::TimeCharacteristic(Type type, AttributeField field, TimeUnit unit)
    : type(type), field(std::move(field)), unit(std::move(unit))
{
}
TimeCharacteristic TimeCharacteristic::createEventTime(LogicalFunction field)
{
    return createEventTime(std::move(field), TimeUnit(1));
}

TimeCharacteristic TimeCharacteristic::createEventTime(LogicalFunction fieldValue, const TimeUnit& unit)
{
    const auto& fieldAccess = fieldValue.get<FieldAccessLogicalFunction>();
    auto keyField = AttributeField(fieldAccess.getFieldName(), fieldAccess.getStamp());
    return TimeCharacteristic(Type::EventTime, keyField, unit);
}

TimeCharacteristic TimeCharacteristic::createIngestionTime()
{
    return TimeCharacteristic(Type::IngestionTime);
}

AttributeField TimeCharacteristic::getField() const
{
    return field;
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

std::string TimeCharacteristic::toString() const
{
    std::stringstream ss;
    ss << "TimeCharacteristic: ";
    ss << " type=" << getTypeAsString();
    ss << " field=" << field.toString();
    ss << std::endl;
    return ss.str();
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

bool TimeCharacteristic::operator==(const TimeCharacteristic& other) const
{
    const bool equalField = (this->field.isEqual(other.field));

    return this->type == other.type && equalField && (this->unit == other.unit);
}

uint64_t TimeCharacteristic::hash() const
{
    uint64_t hashValue = 0;
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint8_t>{}((unsigned char)type);
    hashValue = hashValue * 0x9e3779b1 + field.hash();
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint64_t>{}(unit.getMillisecondsConversionMultiplier());
    return hashValue;
}

}
