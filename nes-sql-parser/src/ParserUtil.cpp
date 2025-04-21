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
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::API
{
Windowing::TimeMeasure Milliseconds(const uint64_t milliseconds)
{
    return Windowing::TimeMeasure(milliseconds);
}

Windowing::TimeMeasure Seconds(const uint64_t seconds)
{
    return Milliseconds(seconds * 1000);
}

Windowing::TimeMeasure Minutes(const uint64_t minutes)
{
    return Seconds(minutes * 60);
}

Windowing::TimeMeasure Hours(const uint64_t hours)
{
    return Minutes(hours * 60);
}

Windowing::TimeMeasure Days(const uint64_t days)
{
    return Hours(days * 24);
}

Windowing::TimeUnit Milliseconds()
{
    return Windowing::TimeUnit::Milliseconds();
}

Windowing::TimeUnit Seconds()
{
    return Windowing::TimeUnit::Seconds();
}

Windowing::TimeUnit Minutes()
{
    return Windowing::TimeUnit::Minutes();
}

Windowing::TimeUnit Hours()
{
    return Windowing::TimeUnit::Hours();
}

Windowing::TimeUnit Days()
{
    return Windowing::TimeUnit::Days();
}

Windowing::TimeCharacteristic EventTime(std::unique_ptr<FieldAccessLogicalFunction> onField)
{
    return Windowing::TimeCharacteristic::createEventTime(std::move(onField));
}

Windowing::TimeCharacteristic EventTime(std::unique_ptr<FieldAccessLogicalFunction> onField, const Windowing::TimeUnit& unit)
{
    return Windowing::TimeCharacteristic::createEventTime(std::move(onField), unit);
}

Windowing::TimeCharacteristic IngestionTime()
{
    return Windowing::TimeCharacteristic::createIngestionTime();
}

}
