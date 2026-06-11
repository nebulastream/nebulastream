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
#include <cstdint>
#include <functional>
#include <ostream>
#include <Time/IntervalBound.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::Windowing
{

/// Closed bound range [lowerBound, upperBound] of a streaming interval join: anchor tuple `a` pairs
/// with partner tuples `b` where `b.ts in [a.ts + lowerBound, a.ts + upperBound]`. Bounds are signed
/// ms, so the range may be past/future-anchored (e.g. [-4, -1]).
class IntervalWindow
{
public:
    IntervalWindow(IntervalBound lowerBound, IntervalBound upperBound);

    [[nodiscard]] IntervalBound getLowerBound() const;
    [[nodiscard]] IntervalBound getUpperBound() const;
    /// The join's slice width: upperBound - lowerBound (always positive, lowerBound < upperBound).
    [[nodiscard]] TimeMeasure getSize() const;

    bool operator==(const IntervalWindow& otherWindowType) const;
    friend std::ostream& operator<<(std::ostream& os, const IntervalWindow& intervalWindow);

private:
    IntervalBound lowerBound;
    IntervalBound upperBound;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::IntervalWindow>
{
    Reflected operator()(const Windowing::IntervalWindow& intervalWindow, const ReflectionContext& context) const;
};

template <>
struct Unreflector<Windowing::IntervalWindow>
{
    Windowing::IntervalWindow operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

namespace detail
{
struct ReflectedIntervalWindow
{
    std::int64_t lowerBound{0};
    std::int64_t upperBound{0};
};
}
}

template <>
struct std::hash<NES::Windowing::IntervalWindow>
{
    std::size_t operator()(const NES::Windowing::IntervalWindow& window) const noexcept;
};

FMT_OSTREAM(NES::Windowing::IntervalWindow);
