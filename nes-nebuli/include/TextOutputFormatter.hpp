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
#include <algorithm>
#include <array>
#include <cstddef>
#include <functional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

template <size_t N, typename... Ts>
std::string toText(const std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>& resultTupleType)
{
    std::stringstream stringBuilder;
    std::array<std::size_t, N> columnWidths{};
    std::vector<std::array<std::string, N>> rows;
    std::array<std::string, N> columnHeader;
    std::ranges::copy(
        resultTupleType.first | views::enumerate
            | std::views::transform(
                [&](const auto& pair)
                {
                    columnWidths[std::get<0>(pair)] = std::get<1>(pair).size();
                    return std::string{std::get<1>(pair)};
                }),
        columnHeader.begin());
    for (const auto& row : resultTupleType.second)
    {
        std::array<std::string, N> currentRow;
        [&]<size_t... Is>(std::index_sequence<Is...>)
        {
            auto testTuple = std::make_tuple(std::get<Is>(row)...);
            auto testStringTuple = std::make_tuple(fmt::format("{}", std::get<Is>(testTuple))...);
            ((currentRow[Is] = fmt::format("{}", std::get<Is>(row))), ...);
            ((columnWidths[Is] = std::max(columnWidths[Is], currentRow[Is].size())), ...);
        }(std::make_index_sequence<sizeof...(Ts)>());
        rows.emplace_back(currentRow);
    }

    auto printRow = [&stringBuilder, &columnWidths](const std::array<std::string, N>& row)
    {
        for (size_t i = 0; i < (N - 1); ++i)
        {
            stringBuilder << fmt::format("{:<{}s} | ", row.at(i), columnWidths.at(i));
        }
        stringBuilder << fmt::format("{:<{}s}", row[N - 1], columnWidths[N - 1]);
        stringBuilder << '\n';
    };

    printRow(columnHeader);
    /// Length of a separation line is the columns widths plus space for the separators
    /// NOLINTNEXTLINE(misc-include-cleaner)
    auto totalLength = std::ranges::fold_left(columnWidths, 0, std::plus()) + ((N - 1) * 3);
    stringBuilder << std::string(totalLength, '-') << '\n';

    for (const auto& row : rows)
    {
        printRow(row);
    }
    return stringBuilder.str();
}
}
