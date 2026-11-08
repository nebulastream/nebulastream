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


#include <WindowTypes/Types/SessionWindow.hpp>

#include <cstddef>
#include <functional>
#include <ostream>
#include <Util/Reflection.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

SessionWindow::SessionWindow(const TimeMeasure gap) : gap(gap)
{
}

TimeMeasure SessionWindow::getGap() const
{
    return gap;
}

std::ostream& operator<<(std::ostream& os, const SessionWindow& sessionWindow)
{
    return os << fmt::format("SessionWindow: gap={}", sessionWindow.getGap());
}

bool SessionWindow::operator==(const SessionWindow& otherWindowType) const = default;

}

namespace NES
{

Reflected
Reflector<Windowing::SessionWindow>::operator()(const Windowing::SessionWindow& sessionWindow, const ReflectionContext& context) const
{
    return context.reflect(sessionWindow.getGap());
}

Windowing::SessionWindow
Unreflector<Windowing::SessionWindow>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return Windowing::SessionWindow{context.unreflect<Windowing::TimeMeasure>(reflected)};
}
}

std::size_t std::hash<NES::Windowing::SessionWindow>::operator()(const NES::Windowing::SessionWindow& window) const noexcept
{
    return std::hash<NES::Windowing::TimeMeasure>{}(window.getGap());
}
