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
#include <memory>
#include <Util/ReflectionFwd.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>

namespace NES::Windowing
{

/// A SlidingWindow assigns records to multiple overlapping windows.
class SlidingWindow
{
public:
    SlidingWindow(TimeMeasure size, TimeMeasure slide);

    [[nodiscard]] TimeMeasure getSize() const;
    [[nodiscard]] TimeMeasure getSlide() const;

    bool operator==(const SlidingWindow& otherWindowType) const;
    friend std::ostream& operator<<(std::ostream& os, const SlidingWindow& slidingWindow);

private:
    TimeMeasure size;
    TimeMeasure slide;
};

}

namespace NES
{
template <>
struct Reflector<Windowing::SlidingWindow>
{
    Reflected operator()(const Windowing::SlidingWindow& slidingWindow) const;
};

template <>
struct Unreflector<Windowing::SlidingWindow>
{
    Windowing::SlidingWindow operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

namespace detail
{
struct ReflectedSlidingWindow
{
    Windowing::TimeMeasure size{0};
    Windowing::TimeMeasure slide{0};
};
}
}

template <>
struct std::hash<NES::Windowing::SlidingWindow>
{
    std::size_t operator()(const NES::Windowing::SlidingWindow& window) const noexcept;
};

FMT_OSTREAM(NES::Windowing::SlidingWindow);
