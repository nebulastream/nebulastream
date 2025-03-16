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
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Types/SlidingWindow.hpp>
#include <Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing
{

SlidingWindow::SlidingWindow(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size, TimeMeasure slide)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size)), slide(std::move(slide))
{
}

std::shared_ptr<WindowType> SlidingWindow::of(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size, TimeMeasure slide)
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
    std::stringstream ss;
    ss << "SlidingWindow: size=" << size.getTime();
    ss << " slide=" << slide.getTime();
    ss << " timeCharacteristic=" << timeCharacteristic;
    ss << std::endl;
    return ss.str();
}

bool SlidingWindow::equal(std::shared_ptr<WindowType> otherWindowType)
{
    if (auto otherSlidingWindow = std::dynamic_pointer_cast<SlidingWindow>(otherWindowType))
    {
        return this->size.equals(otherSlidingWindow->size) && this->slide.equals(otherSlidingWindow->slide)
            && *this->timeCharacteristic == *otherSlidingWindow->timeCharacteristic;
    }
    return false;
}

}
