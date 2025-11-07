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

#ifdef _POSIX_VERSION
    #include <unistd.h>
    #include <sys/ioctl.h>
#endif

namespace NES
{

namespace detail
{

constexpr std::string_view COLUMN_SEPARATOR = " | ";

constexpr auto DEFAULT_MAX_TABLE_WIDTH = 150; /// fallback if current terminal width cant be inferred

/// helper function for determining the width of the terminal window (if NES is running in one)
/// only supported on POSIX systems
/// otherwise default to DEFAULT_MAX_TABLE_WIDTH
inline size_t getMaxTableWidth()
{
#ifdef _POSIX_VERSION
    winsize wsize{};

    const auto success = ioctl(STDOUT_FILENO, TIOCGWINSZ, &wsize); /// NOLINT
    if (success == 0)
    {
        return wsize.ws_col;
    }
#endif
    return DEFAULT_MAX_TABLE_WIDTH;
}

/// remove characters that could produce a display length larger than 1 (e.g. tabs)
inline std::string cleanString(const std::string& inputString)
{
    std::string cleaned;
    cleaned.reserve(inputString.size());
    for (const char character : inputString)
    {
        if (character == '\t')
        {
            cleaned += ' ';
        }
        else if (character == '\n' || character >= ' ')
        {
            cleaned += character;
        }
    }
    return cleaned;
}

/// turn string into multiple lines with at max columnWidth length
inline std::vector<std::string> wrapString(const std::string& inputString, const size_t columnWidth)
{
    std::vector<std::string> lines;
    size_t currentPosition = 0;
    while (currentPosition < inputString.size()) /// walk through the input string
    {
        /// look for next existing line-break from current position
        auto nextLB = inputString.find('\n', currentPosition);
        if (nextLB == std::string::npos)
        {
            nextLB = inputString.size();
        }

        if (nextLB == currentPosition) /// edge case where current line begins with another line break
        {
            currentPosition += 1;
        }
        else if ((nextLB - currentPosition) + 1 <= columnWidth) /// if there is an existing line break, split there
        {
            lines.emplace_back(inputString.substr(currentPosition, nextLB - currentPosition));
            currentPosition = nextLB + 1;
        }
        else /// otherwise need to create new line break
        {
            /// try to line break at a whitespace (or underscore) if there is a viable candidate
            if (const auto lastWhitespace = inputString.find_last_of(" _", currentPosition + columnWidth - 1);
                lastWhitespace != std::string::npos
                && lastWhitespace > currentPosition + (3 * columnWidth / 4)) /// only consider whitespaces in the last quarter of the string
            {
                lines.emplace_back(inputString.substr(currentPosition, lastWhitespace - currentPosition + 1));
                currentPosition = lastWhitespace + 1;
            }
            else
            {
                lines.emplace_back(inputString.substr(currentPosition, columnWidth));
                currentPosition = currentPosition + columnWidth;
            }
        }
    }
    if (lines.empty())
    {
        lines.emplace_back("");
    }
    return lines;
}

template <size_t N>
size_t getTotalTableWidth(std::array<std::size_t, N>& columnWidths)
{
    /// account for text + column seperator characters
    return std::ranges::fold_left(columnWidths, 0, std::plus()) + ((N - 1) * COLUMN_SEPARATOR.size());
}

/// Try to fit the table width into the terminal by narrowing columns
/// If it's not possible to fit all columns (because there are just too many), this function returns how many columns should be printed
template <size_t N>
size_t tryFitTable(std::array<std::size_t, N>& columnWidths)
{
    constexpr size_t minColumnWidth = 1;
    const size_t maxTableWidth = std::max(getMaxTableWidth(), minColumnWidth);
    for (auto& width : columnWidths)
    {
        /// initially clip very long column widths to limit number of iterations
        width = std::clamp(width, static_cast<std::size_t>(0), maxTableWidth);
    }

    auto currentTableWidth = getTotalTableWidth(columnWidths);

    /// Find current widest column, and narrow it to be 1 shorter than the second-widest column (or less if that's enough)
    /// Repeat until the total table width is small enough, or the minimum width has been reached for all columns
    /// Goal is to keep line breaks away from narrow columns for readability
    while (currentTableWidth > maxTableWidth)
    {
        auto widestColumnWidth = std::max_element(columnWidths.begin(), columnWidths.end());
        if (*widestColumnWidth <= minColumnWidth)
        {
            break;
        }
        auto tmp = *widestColumnWidth;
        *widestColumnWidth = 0;
        auto secondWidestColumn = std::max_element(columnWidths.begin(), columnWidths.end());
        auto narrow = std::min(std::min(1 + tmp - *secondWidestColumn, currentTableWidth - maxTableWidth), tmp - minColumnWidth);
        *widestColumnWidth = tmp - narrow;
        currentTableWidth -= narrow;
    }


    auto numColumnsToDisplay = N;
    if (currentTableWidth > maxTableWidth) /// for really wide, undisplayable tables, cut off the columns at the back
    {
        numColumnsToDisplay = 1 + ((maxTableWidth - minColumnWidth) / (minColumnWidth + COLUMN_SEPARATOR.size()));
    }
    return numColumnsToDisplay;
}
}

template <size_t N, typename... Ts>
std::string toText(const std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>& resultTupleType)
{
    std::stringstream stringBuilder;
    std::array<size_t, N> columnWidths{};
    std::vector<std::array<std::string, N>> rows;
    std::array<std::string, N> columnHeader;
    std::ranges::copy(
        resultTupleType.first | views::enumerate
            | std::views::transform(
                [&](const auto& pair)
                {
                    columnWidths.at(std::get<0>(pair)) = std::get<1>(pair).size();
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

    auto numColumnsToDisplay = detail::tryFitTable<N>(columnWidths);

    auto printRow = [&stringBuilder, &columnWidths, &numColumnsToDisplay](const std::array<std::string, N>& row)
    {
        /// Iterate over columns in row and add create multiple lines as necessary
        std::array<std::vector<std::string>, N> wrappedColumns;
        size_t numLines = 0;
        for (size_t i = 0; i < numColumnsToDisplay; ++i)
        {
            auto cleaned = detail::cleanString(row.at(i));
            wrappedColumns.at(i) = detail::wrapString(cleaned, columnWidths.at(i));
            numLines = std::max(numLines, wrappedColumns.at(i).size());
        }

        /// Print each line of the row
        /// Vertical padding of each cell up to the number of target lines
        for (size_t line = 0; line < numLines; ++line)
        {
            for (size_t col = 0; col < numColumnsToDisplay; ++col)
            {
                auto& cellLines = wrappedColumns.at(col);
                const auto& text = line < cellLines.size() ? cellLines[line] : std::string("");
                stringBuilder << fmt::format("{:<{}s}", text, columnWidths.at(col));
                if (col < numColumnsToDisplay - 1)
                {
                    stringBuilder << detail::COLUMN_SEPARATOR;
                }
            }
            stringBuilder << '\n';
        }
    };
    stringBuilder << "\n";
    printRow(columnHeader);

    /// Length of a separation line is the columns widths plus space for the separators
    /// NOLINTNEXTLINE(misc-include-cleaner)
    auto totalLength = detail::getTotalTableWidth(columnWidths);
    stringBuilder << std::string(totalLength, '=') << '\n';

    for (const auto& row : rows)
    {
        printRow(row);
        stringBuilder << std::string(totalLength, '-') << '\n';
    }
    stringBuilder << "\n";
    return stringBuilder.str();
}
}
