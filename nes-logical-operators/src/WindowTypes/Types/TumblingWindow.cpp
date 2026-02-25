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
#include <WindowTypes/Types/TumblingWindow.hpp>

#include <string>
#include <utility>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

TumblingWindow::TumblingWindow(TimeCharacteristic timeCharacteristic, TimeMeasure size)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size))
{
}

TimeMeasure TumblingWindow::getSize() const
{
    return size;
}

TimeMeasure TumblingWindow::getSlide() const
{
    return getSize();
}

std::string TumblingWindow::toString() const
{
    return fmt::format("TumblingWindow: size={} timeCharacteristic={}", size.getTime(), timeCharacteristic);
}

bool TumblingWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* other = dynamic_cast<const TumblingWindow*>(&otherWindowType))
    {
        return (this->size == other->size) && (this->timeCharacteristic == (other->timeCharacteristic));
    }
    return false;
}

}

namespace NES
{

Reflected Reflector<Windowing::TumblingWindow>::operator()(const Windowing::TumblingWindow& tumblingWindow) const
{
    return reflect(
        detail::ReflectedTumblingWindow{.size = tumblingWindow.getSize(), .timeCharacteristic = tumblingWindow.getTimeCharacteristic()});
}

Windowing::TumblingWindow
Unreflector<Windowing::TumblingWindow>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [size, timeCharacteristics] = context.unreflect<detail::ReflectedTumblingWindow>(reflected);
    return {timeCharacteristics, size};
}
}
