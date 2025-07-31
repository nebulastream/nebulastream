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

#include <utility>

#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ErrorHandling.hpp>
#include "Util/Overloaded.hpp"
#include "Util/Reflection.hpp"

namespace NES::Windowing
{
TimeBasedWindowType::TimeBasedWindowType(std::variant<TumblingWindow, SlidingWindow> underlying) : underlying(std::move(underlying))
{
}

bool TimeBasedWindowType::operator==(const TimeBasedWindowType& otherWindowType) const = default;

std::ostream& operator<<(std::ostream& os, const TimeBasedWindowType& windowType)
{
    std::visit([&](const auto& window) { os << window; }, windowType.underlying);
    return os;
}

TimeMeasure TimeBasedWindowType::getSize() const
{
    return std::visit([](const auto& window) { return window.getSize(); }, underlying);
}

TimeMeasure TimeBasedWindowType::getSlide() const
{
    return std::visit(
        Overloaded{
            [](const TumblingWindow& tumblingWindow) { return tumblingWindow.getSize(); },
            [](const SlidingWindow& slidingWindow) { return slidingWindow.getSlide(); }},
        underlying);
}

const std::variant<TumblingWindow, SlidingWindow>& TimeBasedWindowType::getUnderlying() const
{
    return underlying;
}
}

namespace NES
{

Reflected Reflector<Windowing::TimeBasedWindowType>::operator()(const Windowing::TimeBasedWindowType& window) const
{
    return reflect(window.getUnderlying());
}

Windowing::TimeBasedWindowType
Unreflector<Windowing::TimeBasedWindowType>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return Windowing::TimeBasedWindowType{context.unreflect<std::variant<Windowing::TumblingWindow, Windowing::SlidingWindow>>(reflected)};
}
}

std::size_t std::hash<NES::Windowing::TimeBasedWindowType>::operator()(const NES::Windowing::TimeBasedWindowType& window) const noexcept
{
    return std::hash<std::variant<NES::Windowing::TumblingWindow, NES::Windowing::SlidingWindow>>{}(window.getUnderlying());
}
