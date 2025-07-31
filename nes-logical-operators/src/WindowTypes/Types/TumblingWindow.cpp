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

#include <string>
#include <utility>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

TumblingWindow::TumblingWindow(TimeMeasure size) : size(std::move(size))
{
}

TimeMeasure TumblingWindow::getSize()
{
    return size;
}

TimeMeasure TumblingWindow::getSlide()
{
    return getSize();
}

size_t TumblingWindow::hash() const
{
    return std::hash<TimeMeasure>{}(size);
}

std::string TumblingWindow::toString() const
{
    return fmt::format("TumblingWindow: size={}", size.getTime());
}

bool TumblingWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* other = dynamic_cast<const TumblingWindow*>(&otherWindowType))
    {
        return (this->size == other->size);
    }
    return false;
}
}

std::size_t std::hash<NES::Windowing::TumblingWindow>::operator()(const NES::Windowing::TumblingWindow& window) const noexcept
{
    return std::hash<NES::Windowing::TimeMeasure>{}(window.size);
}