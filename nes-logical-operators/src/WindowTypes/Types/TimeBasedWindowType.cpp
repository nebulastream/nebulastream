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

#include <cstddef>
#include <functional>
#include <ostream>
#include <utility>
#include <variant>

#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/IntervalWindow.hpp>
#include <WindowTypes/Types/SessionWindow.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <ErrorHandling.hpp>

namespace NES::Windowing
{
TimeBasedWindowType::TimeBasedWindowType(Underlying underlying) : underlying(std::move(underlying))
{
}

bool TimeBasedWindowType::operator==(const TimeBasedWindowType& otherWindowType) const = default;

std::ostream& operator<<(std::ostream& os, const TimeBasedWindowType& windowType)
{
    std::visit([&](const auto& window) { os << window; }, windowType.underlying);
    return os;
}

bool TimeBasedWindowType::isUnbounded() const
{
    return std::holds_alternative<UnboundedWindow>(underlying);
}

bool TimeBasedWindowType::isSession() const
{
    return std::holds_alternative<SessionWindow>(underlying);
}

TimeMeasure TimeBasedWindowType::getSize() const
{
    return std::visit(
        Overloaded{
            [](const UnboundedWindow&) -> TimeMeasure { throw UnknownWindowType("An unbounded window has no size"); },
            [](const SessionWindow&) -> TimeMeasure { throw UnknownWindowType("A session window has no size"); },
            [](const auto& window) { return window.getSize(); }},
        underlying);
}

TimeMeasure TimeBasedWindowType::getSlide() const
{
    return std::visit(
        Overloaded{
            [](const TumblingWindow& tumblingWindow) { return tumblingWindow.getSize(); },
            [](const SlidingWindow& slidingWindow) { return slidingWindow.getSlide(); },
            [](const IntervalWindow& intervalWindow) { return intervalWindow.getSize(); },
            [](const UnboundedWindow&) -> TimeMeasure { throw UnknownWindowType("An unbounded window has no slide"); },
            [](const SessionWindow&) -> TimeMeasure { throw UnknownWindowType("A session window has no slide"); }},
        underlying);
}

const TimeBasedWindowType::Underlying& TimeBasedWindowType::getUnderlying() const
{
    return underlying;
}
}

namespace NES
{

Reflected
Reflector<Windowing::TimeBasedWindowType>::operator()(const Windowing::TimeBasedWindowType& window, const ReflectionContext& context) const
{
    return context.reflect(window.getUnderlying());
}

Windowing::TimeBasedWindowType
Unreflector<Windowing::TimeBasedWindowType>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return Windowing::TimeBasedWindowType{context.unreflect<Windowing::TimeBasedWindowType::Underlying>(reflected)};
}
}

std::size_t std::hash<NES::Windowing::TimeBasedWindowType>::operator()(const NES::Windowing::TimeBasedWindowType& window) const noexcept
{
    return std::hash<NES::Windowing::TimeBasedWindowType::Underlying>{}(window.getUnderlying());
}
