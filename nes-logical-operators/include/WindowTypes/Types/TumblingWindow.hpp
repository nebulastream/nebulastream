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

#include <string>
#include <Util/ReflectionFwd.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::Windowing
{
/// A TumblingWindow assigns records to non-overlapping windows.
class TumblingWindow
{
public:
    /// Creates a new TumblingWindow that assigns
    /// elements to time windows based on the element timestamp and multiplier.
    /// For example, if you want window a stream by hour,but window begins at the 15th minutes
    /// of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
    /// time windows start at 0:15:00,1:15:00,2:15:00,etc.
    /// @param size
    /// @return std::shared_ptr<WindowType>
    explicit TumblingWindow(TimeMeasure size);
    [[nodiscard]] TimeMeasure getSize() const;
    bool operator==(const TumblingWindow& otherWindowType) const;
    friend std::ostream& operator<<(std::ostream& os, const TumblingWindow& tumblingWindow);

private:
    TimeMeasure size;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::TumblingWindow>
{
    Reflected operator()(const Windowing::TumblingWindow& tumblingWindow) const;
};

template <>
struct Unreflector<Windowing::TumblingWindow>
{
    Windowing::TumblingWindow operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::Windowing::TumblingWindow>
{
    std::size_t operator()(const NES::Windowing::TumblingWindow& window) const noexcept;
};

FMT_OSTREAM(NES::Windowing::TumblingWindow);
