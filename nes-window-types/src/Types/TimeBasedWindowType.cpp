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
#include <API/Schema.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

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
        timeCharacteristic->value.inferStamp(schema);
        if (timeCharacteristic->value.getStamp().value().name() != "Integer")
        {
            throw DifferentFieldTypeExpected("TimeBasedWindow should use a uint for time field {}", timeCharacteristic->value);
        }
    }
    return true;
}

std::shared_ptr<TimeCharacteristic> TimeBasedWindowType::getTimeCharacteristic() const
{
    return timeCharacteristic;
}

}
