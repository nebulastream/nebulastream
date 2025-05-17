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
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>


namespace NES::Windowing
{

TimeCharacteristic::TimeCharacteristic(Type type) : type(type), unit(TimeUnit(1))
{
}

TimeCharacteristic::TimeCharacteristic(Type type, std::shared_ptr<AttributeField> field, TimeUnit unit)
    : field(std::move(field)), type(type), unit(std::move(unit))
{
}
TimeCharacteristic TimeCharacteristic::createEventTime(const FieldAccessLogicalFunction& fieldAccess)
{
    return createEventTime(fieldAccess, TimeUnit(1));
}

TimeCharacteristic TimeCharacteristic::createEventTime(const FieldAccessLogicalFunction& fieldAccess, const TimeUnit& unit)
{
    auto keyField = std::make_shared<AttributeField>(fieldAccess.getFieldName(), fieldAccess.getDataType());
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

bool TimeCharacteristic::operator==(const TimeCharacteristic& other) const
{
    const auto fieldsEqual = (this->field == nullptr && other.field == nullptr)
        || (this->field != nullptr && other.field != nullptr && this->field->isEqual(*other.field));
    return fieldsEqual && type == other.type && unit == other.unit;
}

std::ostream& operator<<(std::ostream& os, const TimeCharacteristic& timeCharacteristic)
{
    std::string fieldString = (timeCharacteristic.field != nullptr) ? timeCharacteristic.field->toString() : "NONE";
    return os << fmt::format("TimeCharacteristic(type: {}, field: {})", timeCharacteristic.getTypeAsString(), fieldString);
}
}
