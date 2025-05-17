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

#include <cstdint>
#include <ostream>
#include <string>
#include <API/TimeUnit.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

TimeUnit::TimeUnit(const uint64_t offset) : multiplier(offset) { };
TimeUnit::TimeUnit(const TimeUnit& timeUnit) = default;

uint64_t TimeUnit::getMillisecondsConversionMultiplier() const
{
    return multiplier;
}

std::ostream& operator<<(std::ostream& os, const TimeUnit& timeUnit)
{
    os << fmt::format("TimeUnit: multiplier= {}", std::to_string(timeUnit.multiplier));
    return os;
}

bool TimeUnit::operator==(const TimeUnit& other) const
{
    return this->multiplier == other.multiplier;
}

TimeUnit TimeUnit::Milliseconds()
{
    return TimeUnit(1);
}

TimeUnit TimeUnit::Seconds()
{
    return TimeUnit(1000);
}

TimeUnit TimeUnit::Minutes()
{
    return TimeUnit(1000 * 60);
}

TimeUnit TimeUnit::Hours()
{
    return TimeUnit(1000 * 60 * 60);
}

TimeUnit TimeUnit::Days()
{
    return TimeUnit(1000 * 60 * 60 * 24);
}

}
