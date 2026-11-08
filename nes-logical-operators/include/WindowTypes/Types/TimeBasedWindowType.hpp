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

#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <variant>
#include <vector>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

#include <Util/Logger/Formatter.hpp>
#include <WindowTypes/Types/IntervalWindow.hpp>
#include <WindowTypes/Types/SessionWindow.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/UnboundedWindow.hpp>

namespace NES::Windowing
{

/// A wrapper around a variant of tumbling, sliding, interval, unbounded and session time-based windows for convenience
struct TimeBasedWindowType
{
    using Underlying = std::variant<TumblingWindow, SlidingWindow, IntervalWindow, UnboundedWindow, SessionWindow>;

    explicit TimeBasedWindowType(Underlying underlying);
    [[nodiscard]] bool isUnbounded() const;
    [[nodiscard]] bool isSession() const;

    /// @brief method to get the window size; unbounded and session windows have none
    /// @return size of window
    [[nodiscard]] TimeMeasure getSize() const;

    /// @brief method to get the window slide
    /// @return slide of the window
    [[nodiscard]] TimeMeasure getSlide() const;
    bool operator==(const TimeBasedWindowType& otherWindowType) const;
    friend std::ostream& operator<<(std::ostream& os, const TimeBasedWindowType& windowType);
    [[nodiscard]] const Underlying& getUnderlying() const;

private:
    Underlying underlying;
};
}

namespace NES
{
template <>
struct Reflector<Windowing::TimeBasedWindowType>
{
    Reflected operator()(const Windowing::TimeBasedWindowType& window, const ReflectionContext& context) const;
};

template <>
struct Unreflector<Windowing::TimeBasedWindowType>
{
    Windowing::TimeBasedWindowType operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::Windowing::TimeBasedWindowType>
{
    std::size_t operator()(const NES::Windowing::TimeBasedWindowType& window) const noexcept;
};

FMT_OSTREAM(NES::Windowing::TimeBasedWindowType);
