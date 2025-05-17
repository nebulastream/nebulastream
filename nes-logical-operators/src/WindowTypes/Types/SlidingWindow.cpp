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

#include <memory>
#include <utility>
#include <API/AttributeField.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/SlidingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

SlidingWindow::SlidingWindow(TimeCharacteristic timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size)), slide(std::move(slide))
{
}

std::shared_ptr<WindowType> SlidingWindow::of(TimeCharacteristic timeCharacteristic, TimeMeasure size, TimeMeasure slide)
{
    return std::make_shared<SlidingWindow>(SlidingWindow(std::move(timeCharacteristic), std::move(size), std::move(slide)));
}

TimeMeasure SlidingWindow::getSize()
{
    return size;
}

TimeMeasure SlidingWindow::getSlide()
{
    return slide;
}

std::string SlidingWindow::toString() const
{
    return fmt::format("SlidingWindow: size={} slide={} timeCharacteristic={}", size.getTime(), slide.getTime(), timeCharacteristic);
}

bool SlidingWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* otherSlidingWindow = dynamic_cast<const SlidingWindow*>(&otherWindowType))
    {
        return (this->size == otherSlidingWindow->size) && (this->slide == otherSlidingWindow->slide)
            && (this->timeCharacteristic == (otherSlidingWindow->timeCharacteristic));
    }
    return false;
}

}
