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

#include <WindowTypes/Types/SlidingWindow.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>

namespace NES::Windowing
{

SlidingWindow::SlidingWindow(TimeMeasure size, TimeMeasure slide) : size(std::move(size)), slide(std::move(slide))
{
}

TimeMeasure SlidingWindow::getSize() const
{
    return size;
}

TimeMeasure SlidingWindow::getSlide() const
{
    return slide;
}

std::ostream& operator<<(std::ostream& os, const SlidingWindow& slidingWindow)
{
    return os << fmt::format("SlidingWindow: size={} slide={}", slidingWindow.getSize(), slidingWindow.getSlide());
}

bool SlidingWindow::operator==(const SlidingWindow& otherWindowType) const = default;

}

std::size_t std::hash<NES::Windowing::SlidingWindow>::operator()(const NES::Windowing::SlidingWindow& window) const noexcept
{
    return folly::hash::hash_combine(window.getSize(), window.getSlide());
}

namespace NES
{

Reflected Reflector<Windowing::SlidingWindow>::operator()(const Windowing::SlidingWindow& slidingWindow) const
{
    return reflect(detail::ReflectedSlidingWindow{.size = slidingWindow.getSize(), .slide = slidingWindow.getSlide()});
}

Windowing::SlidingWindow
Unreflector<Windowing::SlidingWindow>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [size, slide] = context.unreflect<detail::ReflectedSlidingWindow>(reflected);
    return Windowing::SlidingWindow{size, slide};
}
}
