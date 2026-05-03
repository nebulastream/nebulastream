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
#include <magic_enum/magic_enum.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const PhysicalType::Component& component)
{
    return os << fmt::format("Component(suffix: '{}', type: {})", component.suffix, magic_enum::enum_name(component.type));
}

std::ostream& operator<<(std::ostream& os, const PhysicalType& type)
{
    return os << fmt::format("PhysicalType(components: [{}], nullable: {})", fmt::join(type.components, ", "), type.nullable);
}

}
