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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
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

std::shared_ptr<TimeCharacteristic> TimeCharacteristic::createEventTime(const std::shared_ptr<NodeFunction>& field)
{
    return createEventTime(field, TimeUnit(1));
}

std::shared_ptr<TimeCharacteristic>
TimeCharacteristic::createEventTime(const std::shared_ptr<NodeFunction>& fieldValue, const TimeUnit& unit)
{
    if (!NES::Util::instanceOf<NodeFunctionFieldAccess>(fieldValue))
    {
        throw QueryInvalid(fmt::format("Query: window key has to be an FieldAccessFunction but it was a  {}", *fieldValue));
    }
    auto fieldAccess = NES::Util::as<NodeFunctionFieldAccess>(fieldValue);
    const std::shared_ptr<AttributeField> keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<TimeCharacteristic>(Type::EventTime, keyField, unit);
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
    const auto isFieldsEqual = (this->field == nullptr and other.field == nullptr) or (this->field->isEqual(other.field));
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
    std::string fieldString = (timeCharacteristic.field != nullptr) ? timeCharacteristic.field->toString() : "NONE";
    return os << fmt::format("TimeCharacteristic(type: {}, field: {})", timeCharacteristic.getTypeAsString(), fieldString);
}
}
