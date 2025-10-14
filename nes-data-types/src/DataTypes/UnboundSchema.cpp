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

#include <ranges>
#include <unordered_map>
#include <DataTypes/UnboundSchema.hpp>

#include <fmt/ranges.h>

namespace NES
{

bool operator==(const UnboundField& lhs, const UnboundField& rhs)
{
    return lhs.getName() == rhs.getName() && lhs.getDataType() == rhs.getDataType();
}

std::ostream& operator<<(std::ostream& os, const UnboundField& obj)
{
    return os << fmt::format("UnboundField: (name: {}, type: {})", obj.getName(), obj.getDataType());
}

}