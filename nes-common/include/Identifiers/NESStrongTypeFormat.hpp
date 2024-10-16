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

#pragma once
#include <Identifiers/NESStrongType.hpp>
#include <fmt/std.h>

/**
 * Adds NESStrongType overloads for the fmt formatting library.
 * This allows direct formatting of Identifiers like `fmt::format("Query: {}", QueryId)`
 */
namespace fmt
{
template <typename T, typename Tag, T invalid, T initial>
struct formatter<NES::NESStrongType<T, Tag, invalid, initial>> : formatter<std::string>
{
    auto format(const NES::NESStrongType<T, Tag, invalid, initial>& t, format_context& ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "{}", t.getRawValue());
    }
};
}
