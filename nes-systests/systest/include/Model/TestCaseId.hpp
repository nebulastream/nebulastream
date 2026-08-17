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

#include <cstdint>
#include <string>

#include <fmt/base.h>
#include <fmt/format.h>

#include <Model/SystestQueryId.hpp>

namespace NES
{

/// Identifies one case in a run's reports: the test file, which part of it, and the query number within it.
/// A file splits into parts when its queries ask for different worker settings, and the variant tells those parts
/// apart, which the display name alone cannot.
/// An entry about a whole file, such as a failed setup, carries the invalid query number and prints without one.
struct TestCaseId
{
    std::string file;
    uint32_t variant = 0;
    SystestQueryId query = INVALID<SystestQueryId>;

    bool operator==(const TestCaseId& other) const = default;
};

}

template <>
struct fmt::formatter<NES::TestCaseId>
{
    static constexpr auto parse(const format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

    static auto format(const NES::TestCaseId& id, format_context& ctx) -> decltype(ctx.out())
    {
        auto out = fmt::format_to(ctx.out(), "{}", id.file);
        if (id.variant > 0)
        {
            out = fmt::format_to(out, "[{}]", id.variant);
        }
        if (id.query != NES::INVALID<NES::SystestQueryId>)
        {
            out = fmt::format_to(out, ":{:02}", id.query.getRawValue());
        }
        return out;
    }
};

static_assert(fmt::formattable<NES::TestCaseId>);
