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
#include <API/Schema.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Integer.hpp>

namespace NES::Windowing
{

TimeBasedWindowType::TimeBasedWindowType(std::shared_ptr<TimeCharacteristic> timeCharacteristic)
    : timeCharacteristic(std::move(timeCharacteristic))
{
}

bool TimeBasedWindowType::inferStamp(const Schema& schema)
{
    if (timeCharacteristic->getType() == TimeCharacteristic::Type::EventTime)
    {
        auto fieldName = timeCharacteristic->getField()->getName();
        auto existingField = schema.getFieldByName(fieldName);
        if (existingField)
        {
            if (!NES::Util::instanceOf<Integer>(existingField.value()->getDataType()))
            {
                throw DifferentFieldTypeExpected("TimeBasedWindow should use a uint for time field {}", fieldName);
            }
            timeCharacteristic->getField()->setName(existingField.value()->getName());
            return true;
        }
        if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME)
        {
            return true;
        }

        throw DifferentFieldTypeExpected("TimeBasedWindow using a non existing time field {}", fieldName);
    }
    return true;
}

std::shared_ptr<TimeCharacteristic> TimeBasedWindowType::getTimeCharacteristic() const
{
    return timeCharacteristic;
}

}
