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

#include <WindowTypes/Types/CountSlidingWindow.hpp>

#include <memory>
#include <string>
#include <utility>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

CountSlidingWindow::CountSlidingWindow(CountMeasure size, CountMeasure slide) : size(std::move(size)), slide(std::move(slide))
{
}

std::shared_ptr<WindowType> CountSlidingWindow::of(CountMeasure size, CountMeasure slide)
{
    return std::make_shared<CountSlidingWindow>(CountSlidingWindow(std::move(size), std::move(slide)));
}

CountMeasure CountSlidingWindow::getSize() const
{
    return size;
}

CountMeasure CountSlidingWindow::getSlide() const
{
    return slide;
}

std::string CountSlidingWindow::toString() const
{
    return fmt::format("CountSlidingWindow: size={} slide={}", size.getCount(), slide.getCount());
}

bool CountSlidingWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* otherSlidingWindow = dynamic_cast<const CountSlidingWindow*>(&otherWindowType))
    {
        return (this->size == otherSlidingWindow->size) && (this->slide == otherSlidingWindow->slide);
    }
    return false;
}

}

namespace NES
{

Reflected Reflector<Windowing::CountSlidingWindow>::operator()(const Windowing::CountSlidingWindow& slidingWindow) const
{
    return reflect(detail::ReflectedCountSlidingWindow{.size = slidingWindow.getSize(), .slide = slidingWindow.getSlide()});
}

Windowing::CountSlidingWindow Unreflector<Windowing::CountSlidingWindow>::operator()(const Reflected& reflected) const
{
    auto [size, slide] = unreflect<detail::ReflectedCountSlidingWindow>(reflected);
    return Windowing::CountSlidingWindow(size, slide);
}
}