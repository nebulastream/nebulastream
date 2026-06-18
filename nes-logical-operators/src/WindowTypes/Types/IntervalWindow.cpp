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

#include <WindowTypes/Types/IntervalWindow.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ostream>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>

namespace NES::Windowing
{

/// No bound validation here; lowerBound < upperBound is enforced (with a graceful error) by
/// IntervalJoinLogicalOperator so invalid user queries are rejected rather than terminating.
IntervalWindow::IntervalWindow(const std::int64_t lowerBound, const std::int64_t upperBound)
    : lowerBound(lowerBound), upperBound(upperBound)
{
}

std::int64_t IntervalWindow::getLowerBound() const
{
    return lowerBound;
}

std::int64_t IntervalWindow::getUpperBound() const
{
    return upperBound;
}

TimeMeasure IntervalWindow::getSize() const
{
    return TimeMeasure{static_cast<uint64_t>(upperBound - lowerBound)};
}

std::ostream& operator<<(std::ostream& os, const IntervalWindow& intervalWindow)
{
    return os << fmt::format("IntervalWindow: [{}, {}]", intervalWindow.getLowerBound(), intervalWindow.getUpperBound());
}

bool IntervalWindow::operator==(const IntervalWindow& otherWindowType) const = default;

}

std::size_t std::hash<NES::Windowing::IntervalWindow>::operator()(const NES::Windowing::IntervalWindow& window) const noexcept
{
    return folly::hash::hash_combine(window.getLowerBound(), window.getUpperBound());
}

namespace NES
{

Reflected Reflector<Windowing::IntervalWindow>::operator()(const Windowing::IntervalWindow& intervalWindow) const
{
    return reflect(
        detail::ReflectedIntervalWindow{.lowerBound = intervalWindow.getLowerBound(), .upperBound = intervalWindow.getUpperBound()});
}

Windowing::IntervalWindow
Unreflector<Windowing::IntervalWindow>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [lowerBound, upperBound] = context.unreflect<detail::ReflectedIntervalWindow>(reflected);
    return Windowing::IntervalWindow{lowerBound, upperBound};
}
}
