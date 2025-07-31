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
#include <fmt/format.h>

namespace NES::Windowing
{

TumblingWindow::TumblingWindow(TimeMeasure size) : size(std::move(size))
{
}

TimeMeasure TumblingWindow::getSize() const
{
    return size;
}

std::ostream& operator<<(std::ostream& os, const TumblingWindow& tumblingWindow)
{
    return os << fmt::format("TumblingWindow: size={}", tumblingWindow.getSize());
}

bool TumblingWindow::operator==(const TumblingWindow& otherWindowType) const = default;

}

namespace NES
{

Reflected Reflector<Windowing::TumblingWindow>::operator()(const Windowing::TumblingWindow& tumblingWindow) const
{
    return reflect(tumblingWindow.getSize());
}

Windowing::TumblingWindow
Unreflector<Windowing::TumblingWindow>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto size = context.unreflect<Windowing::TimeMeasure>(reflected);
    return Windowing::TumblingWindow{size};
}
}

std::size_t std::hash<NES::Windowing::TumblingWindow>::operator()(const NES::Windowing::TumblingWindow& window) const noexcept
{
    return std::hash<NES::Windowing::TimeMeasure>{}(window.getSize());
}