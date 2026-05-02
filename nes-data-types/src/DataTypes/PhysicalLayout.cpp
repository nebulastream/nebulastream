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

#include <DataTypes/PhysicalLayout.hpp>

#include <ostream>
#include <fmt/format.h>
#include <fmt/ranges.h>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const PhysicalLayout::Component& component)
{
    return os << fmt::format("Component(suffix: '{}', physical: {})", component.suffix, component.physicalType);
}

std::ostream& operator<<(std::ostream& os, const PhysicalLayout& layout)
{
    return os << fmt::format("PhysicalLayout({})", fmt::join(layout.components, ", "));
}

}
