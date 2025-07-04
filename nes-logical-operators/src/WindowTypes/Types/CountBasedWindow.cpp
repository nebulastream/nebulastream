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
#include <WindowTypes/Types/CountBasedWindowType.hpp>
#include <WindowTypes/Types/CountBasedWindow.hpp>
#include <WindowTypes/Types/WindowType.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{

CountBasedWindow::CountBasedWindow(uint64_t size): size(size)
{
}

uint64_t CountBasedWindow::getSize()
{
    return size;
}

std::string CountBasedWindow::toString() const
{
    return fmt::format("CountBasedWindow: size={} ", size);
}

bool CountBasedWindow::operator==(const WindowType& otherWindowType) const
{
    if (const auto* other = dynamic_cast<const CountBasedWindow*>(&otherWindowType))
    {
        return (this->size == other->size);
    }
    return false;
}

}