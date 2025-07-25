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

#include <WindowTypes/Types/TimeBasedWindowType.hpp>

#include <utility>
#include <DataTypes/Schema.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>

namespace NES::Windowing
{

TimeBasedWindowType::TimeBasedWindowType(TimeCharacteristic timeCharacteristic) : timeCharacteristic(std::move(timeCharacteristic))
{
}

bool TimeBasedWindowType::inferStamp(const Schema& schema)
{
    if (timeCharacteristic.getType() == TimeCharacteristic::Type::EventTime)
    {
        auto fieldName = timeCharacteristic.field.name;
        auto existingField = schema.getFieldByName(fieldName);
        if (existingField)
        {
            if (not existingField.value().dataType.isInteger())
            {
                throw DifferentFieldTypeExpected("TimeBasedWindow should use a uint for time field " + fieldName);
            }
            timeCharacteristic.field.name = existingField.value().name;
            return true;
        }
        if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
        {
            return true;
        }
        throw DifferentFieldTypeExpected("TimeBasedWindow using a non existing time field " + fieldName);
    }
    return true;
}

TimeCharacteristic TimeBasedWindowType::getTimeCharacteristic() const
{
    return timeCharacteristic;
}

}
