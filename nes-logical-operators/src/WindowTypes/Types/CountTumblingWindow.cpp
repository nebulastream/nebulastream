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

#include <WindowTypes/Types/CountTumblingWindow.hpp>

#include <string>
#include <utility>
#include <Util/Reflection.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

CountTumblingWindow::CountTumblingWindow(CountMeasure size) : size(std::move(size))
{
}

CountMeasure CountTumblingWindow::getSize() const
{
    return size;
}

CountMeasure CountTumblingWindow::getSlide() const
{
    return getSize();
}

std::string CountTumblingWindow::toString() const
{
    return fmt::format("CountTumblingWindow: size={}", size.getCount());
}

bool CountTumblingWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* other = dynamic_cast<const CountTumblingWindow*>(&otherWindowType))
    {
        return this->size == other->size;
    }
    return false;
}

}

namespace NES
{

Reflected Reflector<Windowing::CountTumblingWindow>::operator()(const Windowing::CountTumblingWindow& tumblingWindow) const
{
    return reflect(detail::ReflectedCountTumblingWindow{.size = tumblingWindow.getSize()});
}

Windowing::CountTumblingWindow Unreflector<Windowing::CountTumblingWindow>::operator()(const Reflected& reflected) const
{
    auto [size] = unreflect<detail::ReflectedCountTumblingWindow>(reflected);
    return Windowing::CountTumblingWindow(size);
}
}