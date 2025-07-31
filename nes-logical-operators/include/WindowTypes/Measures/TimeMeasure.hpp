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
#include <functional>
#include <memory>
#include <ostream>
#include <string>

#include "Util/Logger/Formatter.hpp"

namespace NES::Windowing
{
/// A time based window measure.
class TimeMeasure
{
public:
    constexpr explicit TimeMeasure(const uint64_t milliseconds) : milliSeconds(milliseconds) { }

    [[nodiscard]] constexpr uint64_t getTime() const { return milliSeconds; };

    friend std::ostream& operator<<(std::ostream& ostream, const TimeMeasure& measure);

    constexpr auto operator<=>(const TimeMeasure& other) const = default;

private:
    const uint64_t milliSeconds;
    friend class std::hash<TimeMeasure>;
};

}

FMT_OSTREAM(NES::Windowing::TimeMeasure);

template <>
struct std::hash<NES::Windowing::TimeMeasure>
{
    constexpr std::size_t operator()(const NES::Windowing::TimeMeasure& timeMeasure) const noexcept { return timeMeasure.milliSeconds; }
};
