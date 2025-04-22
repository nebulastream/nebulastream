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
#include <vector>
#include <Measures/TimeCharacteristic.hpp>
#include <Measures/TimeMeasure.hpp>
#include <Types/TumblingWindow.hpp>
#include <Types/WindowType.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing
{

TumblingWindow::TumblingWindow(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size))
{
}

std::shared_ptr<WindowType> TumblingWindow::of(std::shared_ptr<TimeCharacteristic> timeCharacteristic, TimeMeasure size)
{
    return std::dynamic_pointer_cast<WindowType>(
        std::make_shared<TumblingWindow>(TumblingWindow(std::move(timeCharacteristic), std::move(size))));
}

TimeMeasure TumblingWindow::getSize()
{
    return size;
}

TimeMeasure TumblingWindow::getSlide()
{
    return getSize();
}

std::string TumblingWindow::toString() const
{
    std::stringstream ss;
    ss << "TumblingWindow: size=" << size.getTime();
    ss << " timeCharacteristic=" << *timeCharacteristic;
    ss << std::endl;
    return ss.str();
}

bool TumblingWindow::equal(std::shared_ptr<WindowType> otherWindowType)
{
    if (auto otherTumblingWindow = std::dynamic_pointer_cast<TumblingWindow>(otherWindowType))
    {
        return this->size.equals(otherTumblingWindow->size) and (*this->timeCharacteristic == *otherTumblingWindow->timeCharacteristic);
    }
    return false;
}
}
