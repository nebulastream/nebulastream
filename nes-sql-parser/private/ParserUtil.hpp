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

#pragma once

#include <cstdint>
#include <API/TimeUnit.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::API
{
[[maybe_unused]] inline Windowing::TimeCharacteristic IngestionTime()
{
    return Windowing::TimeCharacteristic::createIngestionTime();
}

[[maybe_unused]] inline Windowing::TimeMeasure Milliseconds(uint64_t milliseconds)
{
    return Windowing::TimeMeasure(milliseconds);
}

[[maybe_unused]] inline Windowing::TimeMeasure Seconds(uint64_t seconds)
{
    return Milliseconds(seconds * 1000);
}

[[maybe_unused]] inline Windowing::TimeMeasure Minutes(uint64_t minutes)
{
    return Seconds(minutes * 60);
}

[[maybe_unused]] inline Windowing::TimeMeasure Hours(uint64_t hours)
{
    return Minutes(hours * 60);
}

[[maybe_unused]] inline Windowing::TimeMeasure Days(uint64_t days)
{
    return Hours(days * 24);
}

[[maybe_unused]] inline Windowing::TimeUnit Milliseconds()
{
    return Windowing::TimeUnit::Milliseconds();
}

[[maybe_unused]] inline Windowing::TimeUnit Seconds()
{
    return Windowing::TimeUnit::Seconds();
}

[[maybe_unused]] inline Windowing::TimeUnit Minutes()
{
    return Windowing::TimeUnit::Minutes();
}

[[maybe_unused]] inline Windowing::TimeUnit Hours()
{
    return Windowing::TimeUnit::Hours();
}

[[maybe_unused]] inline Windowing::TimeUnit Days()
{
    return Windowing::TimeUnit::Days();
}

}
