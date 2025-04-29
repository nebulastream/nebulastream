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
#include <string>

namespace NES::Windowing
{

/// A time based window measure.
class TimeUnit
{
public:
    explicit TimeUnit(uint64_t offset);
    ~TimeUnit() = default;
    TimeUnit(const TimeUnit& timeUnit);

    /// @brief gets the multiplier to convert this to milliseconds
    [[nodiscard]] uint64_t getMillisecondsConversionMultiplier() const;

    friend std::ostream& operator<<(std::ostream& os, const TimeUnit& timeUnit);
    [[nodiscard]] bool operator==(const TimeUnit& other) const;

    static TimeUnit Milliseconds();
    static TimeUnit Seconds();
    static TimeUnit Minutes();
    static TimeUnit Hours();
    static TimeUnit Days();

private:
    uint64_t multiplier;
};

}
