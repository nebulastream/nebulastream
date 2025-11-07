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
#include <asm-generic/ioctls.h>
#include <fmt/format.h>
#include <fmt/ranges.h> /// NOLINT(misc-include-cleaner)

#ifdef _WIN32
    #include <windows.h>
#else
    #include <unistd.h>
    #include <sys/ioctl.h>
#endif

namespace NES
{

namespace detail
{

static constexpr auto DEFAULT_MAX_TABLE_WIDTH = 150; /// fallback if current terminal width cant be inferred

inline int getTerminalWidth()
{
#ifdef _WIN32
    CONSOLE_SCREEN_BUFFER_INFO csbi;
    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &csbi))
    {
        return csbi.srWindow.Right - csbi.srWindow.Left + 1;
    }
    return DEFAULT_MAX_TABLE_WIDTH;
#else
    struct winsize wsize{};
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-style-varargs)
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &wsize) == 0)
    {
        return wsize.ws_col;
    }
    return DEFAULT_MAX_TABLE_WIDTH;
#endif
}

/// remove characters that could produce a display length larger than 1 (e.g. tabs)
inline std::string cleanString(const std::string& inputString)
{
    std::string cleaned;
    for (const char c : inputString)
    {
        if (c == '\t')
        {
            cleaned += ' ';
        }
        else if (c == '\n' || c >= ' ')
        {
            cleaned += c;
        }
    }
    return cleaned;
}

/// turn string into multiple lines with at max columnWidth length
inline std::vector<std::string> wrapString(const std::string& inputString, size_t columnWidth)
{
    std::vector<std::string> lines;
    size_t curr = 0;
    while (curr < inputString.size())
    {
        /// look for existing line-break
        auto nextLB = inputString.find('\n', curr);
        if (nextLB == std::string::npos)
        {
            nextLB = inputString.size();
        }
        if (nextLB == curr) /// edge case where line begins with line break
        {
            curr += 1;
        }
        else if (1 + nextLB - curr <= columnWidth) /// if there is an existing line break, split there
        {
            lines.emplace_back(inputString.substr(curr, nextLB - curr));
            curr = nextLB + 1;
        }
        else /// otherwise need to create new line break
        {
            /// try to line break at a whitespace (or underscore) if there is a viable candidate
            auto lastWhitespace = inputString.find_last_of(" _", curr + columnWidth - 1);
            if (lastWhitespace != std::string::npos && lastWhitespace > curr + (3 * columnWidth / 4))
            {
                lines.emplace_back(inputString.substr(curr, 1 + lastWhitespace - curr));
                curr = lastWhitespace + 1;
            }
            else
            {
                lines.emplace_back(inputString.substr(curr, columnWidth));
                curr = curr + columnWidth;
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
size_t getTableWidth(std::array<std::size_t, N>& columnWidths)
{
    return std::ranges::fold_left(columnWidths, 0, std::plus()) + ((N - 1) * 3);
}

/// Try to fit the table width into the terminal by shrinking columns
/// If it's not possible to fit all columns (because there are just too many), this function returns how many columns should be printed
template <size_t N>
size_t tryFitTable(std::array<std::size_t, N>& columnWidths)
{
    /// shrink columns to fit into max row width
    const size_t maxLength = detail::getTerminalWidth();
    const size_t minColumnWidth = 4;
    for (auto& width : columnWidths)
    {
        /// initially clip very long column widths to limit number of iterations
        width = std::clamp(width, static_cast<std::size_t>(0), maxLength);
    }

    auto totalLength = getTableWidth(columnWidths);

    /// Iterative algorithm
    /// Find current widest column, and shrink to be 1 shorter than the second-widest column (or less if that's enough)
    /// Repeat until the total table width is small enough, or the minimum width has been reached for all columns
    /// Goal is to keep line breaks away from narrow columns for readability
    while (totalLength > maxLength)
    {
        auto widestColumn = std::max_element(columnWidths.begin(), columnWidths.end());
        if (*widestColumn <= minColumnWidth)
        {
            break;
        }
        auto tmp = *widestColumn;
        *widestColumn = 0;
        auto secondWidestColumn = std::max_element(columnWidths.begin(), columnWidths.end());
        auto shrink = std::min(std::min(1 + tmp - *secondWidestColumn, totalLength - maxLength), tmp - minColumnWidth);
        *widestColumn = tmp - shrink;
        totalLength -= shrink;
    }

    /// for really wide tables, cut off the columns at the back (?)
    auto numColumnsToDisplay = N;
    if (totalLength > maxLength)
    {
        auto cutoff = (2 + totalLength - maxLength) / 4;
        numColumnsToDisplay = N - cutoff;
    }
    return numColumnsToDisplay;
}
}

template <size_t N, typename... Ts>
std::string toText(const std::pair<std::array<std::string_view, N>, std::vector<std::tuple<Ts...>>>& resultTupleType)
{
    std::stringstream stringBuilder;
    std::array<std::size_t, N> columnWidths;
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

    auto numColumnsToDisplay = detail::tryFitTable<N>(columnWidths);

    auto printRow = [&stringBuilder, &columnWidths, &numColumnsToDisplay](const std::array<std::string, N>& row)
    {
        /// Iterate over columns in row and add create multiple lines as necessary
        std::array<std::vector<std::string>, N> wrappedColumns;
        size_t numLines = 0;
        for (size_t i = 0; i < numColumnsToDisplay; ++i)
        {
            auto cleaned = detail::cleanString(row[i]);
            wrappedColumns[i] = detail::wrapString(cleaned, columnWidths[i]);
            numLines = std::max(numLines, wrappedColumns[i].size());
        }

        /// Print each line of the row
        /// Vertical padding of each cell up to the number of target lines
        for (size_t line = 0; line < numLines; ++line)
        {
            for (size_t col = 0; col < numColumnsToDisplay; ++col)
            {
                auto& cellLines = wrappedColumns[col];
                const auto& text = line < cellLines.size() ? cellLines[line] : std::string("");
                stringBuilder << fmt::format("{:<{}s}", text, columnWidths[col]);
                if (col < numColumnsToDisplay - 1)
                {
                    stringBuilder << " | ";
                }
            }
            stringBuilder << '\n';
        }
    };
    stringBuilder << "\n";
    printRow(columnHeader);

    /// Length of a separation line is the columns widths plus space for the separators
    /// NOLINTNEXTLINE(misc-include-cleaner)
    auto totalLength = detail::getTableWidth(columnWidths);
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
