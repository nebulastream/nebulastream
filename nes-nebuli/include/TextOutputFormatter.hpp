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
#include <ErrorHandling.hpp>

#ifdef _POSIX_VERSION
    #include <unistd.h>
    #include <sys/ioctl.h>
#endif

namespace NES
{

namespace detail
{

constexpr std::string_view COLUMN_SEPARATOR = " | ";

constexpr size_t DEFAULT_MAX_TABLE_WIDTH = 150; /// fallback if current terminal width cant be inferred

/// helper function for determining the width of the terminal window (if NES is running in one)
/// only supported on POSIX systems; otherwise default to DEFAULT_MAX_TABLE_WIDTH
inline size_t getMaxTableWidth()
{
#ifdef _POSIX_VERSION
    winsize wsize{};
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    auto res = ioctl(STDOUT_FILENO, TIOCGWINSZ, &wsize);
    if (res == 0 && wsize.ws_col > 1)
    {
        return wsize.ws_col;
    }
#endif
    return DEFAULT_MAX_TABLE_WIDTH;
}

/// remove characters that could produce a display-length larger than 1 (e.g. tabs)
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
    PRECONDITION(columnWidth >= 1, "strings cant be wrapped to a column width of 0");
    const std::string_view inputView(inputString);
    std::vector<std::string> lines;
    size_t currentPosition = 0;
    while (currentPosition < inputString.size()) /// walk through the input string
    {
        const auto lineView = inputView.substr(currentPosition, columnWidth);

        /// look for existing line break in the line view
        auto nextLB = lineView.find('\n');
        if (nextLB == 0) /// edge case where current line begins with another line break
        {
            lines.emplace_back("");
            currentPosition += 1;
        }
        else if (nextLB != std::string_view::npos) /// there is a line break; split there
        {
            lines.emplace_back(lineView.substr(0, nextLB));
            currentPosition += nextLB + 1;
        }
        else /// otherwise need to create new line break
        {
            /// try to line break at a whitespace (or underscore) if there is a viable candidate
            const auto lastWhitespace = lineView.find_last_of(" _");
            if (lastWhitespace != std::string::npos
                && lastWhitespace > (3 / 4) * columnWidth) /// only consider whitespaces in the last quarter of the string
            {
                lines.emplace_back(lineView.substr(0, lastWhitespace + 1));
                currentPosition += lastWhitespace + 1;
            }
            else
            {
                lines.emplace_back(lineView);
                currentPosition += columnWidth;
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
    if (N == 0)
    {
        return 0;
    }
    /// sum column widths, and add column seperators for present columns (column width > 0)
    return columnWidths.at(0)
        + std::ranges::fold_left(
               columnWidths.begin() + 1,
               columnWidths.end(),
               size_t{0},
               [](size_t acc, const size_t x)
               {
                   if (x == 0)
                   {
                       return acc;
                   }
                   return acc + x + COLUMN_SEPARATOR.size();
               });
}

/// eliminate excess width as far as currently possible
template <size_t N>
void distributeExcessWidth(
    std::array<std::size_t, N>& columnWidths, size_t excessWidth, size_t largestWidth, size_t largestWidthCount, size_t secondLargestWidth)
{
    if (largestWidthCount >= excessWidth)
    {
        /// final iteration; more widest columns than excess with
        for (auto& width : columnWidths)
        {
            if (width == largestWidth && excessWidth--)
            {
                --width;
                INVARIANT(width > 0, "column width cannot be negative; should never happen");
            }
        }
        return;
    }

    size_t newWidth = 1;
    if (largestWidthCount * (largestWidth - secondLargestWidth) < excessWidth)
    {
        /// the widest columns arent yet sufficient to eliminate the entire excess width; reduce to secondLargestWidth
        newWidth = secondLargestWidth;
    }
    else
    {
        /// distribute remaining excess width among widest columns; after that at most 1 more iteration for the remainder
        newWidth = largestWidth - (excessWidth / largestWidthCount);
    }

    INVARIANT(newWidth > 0, "column width cannot be negative; should never happen");
    std::replace(columnWidths.begin(), columnWidths.end(), largestWidth, newWidth);
}

/// Fit the table into the terminal width by narrowing columns
/// Column widths may be set to 0 if the table is too wide to display all columns.
template <size_t N>
void fitTable(std::array<std::size_t, N>& columnWidths)
{
    constexpr size_t minColumnWidth = 4;
    const size_t maxTableWidth = std::max(getMaxTableWidth(), minColumnWidth);

    /// Handle edge case: too many columns to fit; slightly pessimistic for columns smaller than minColumnWidth
    const size_t maxColumns = 1 + ((maxTableWidth - minColumnWidth) / (minColumnWidth + COLUMN_SEPARATOR.size()));
    if (N > maxColumns)
    {
        std::fill(columnWidths.begin(), columnWidths.begin() + maxColumns, minColumnWidth);
        std::fill(columnWidths.begin() + maxColumns, columnWidths.end(), 0);
        return;
    }

    /// Repeat until the excess width is 0; excess width is strictly decreasing per iteration
    while (getTotalTableWidth(columnWidths) > maxTableWidth)
    {
        const size_t excessWidth
            = getTotalTableWidth(columnWidths) - maxTableWidth; /// (dont move this into while condition to avoid underflow)

        /// Find the largest column width and how many times it occurs
        auto largestWidth = *std::max_element(columnWidths.begin(), columnWidths.end());
        size_t largestWidthCount = std::count(columnWidths.begin(), columnWidths.end(), largestWidth);

        if (largestWidth <= minColumnWidth)
        {
            INVARIANT(false, "this should never happen if the previous maxColumns check works correctly");
            break;
        }

        /// Find the next largest distinct column width
        size_t secondLargestWidth = minColumnWidth;
        for (auto& width : columnWidths)
        {
            if (width < largestWidth && width > secondLargestWidth)
            {
                secondLargestWidth = width;
            }
        }

        /// Reduce excess width by narrowing the widest columns
        distributeExcessWidth(columnWidths, excessWidth, largestWidth, largestWidthCount, secondLargestWidth);
    }
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

    detail::fitTable<N>(columnWidths);

    auto printRow = [&stringBuilder, &columnWidths](const std::array<std::string, N>& row)
    {
        /// Iterate over columns in row and add create multiple lines as necessary
        std::vector<std::vector<std::string>> wrappedColumns;
        size_t numLines = 0;
        for (size_t i = 0; i < N; ++i)
        {
            if (columnWidths.at(i) != 0)
            {
                auto cleaned = detail::cleanString(row.at(i));
                wrappedColumns.emplace_back(detail::wrapString(cleaned, columnWidths.at(i)));
                numLines = std::max(numLines, wrappedColumns.at(i).size());
            }
        }

        /// Print each line of the row
        /// Vertical padding of each cell up to the number of target lines
        for (size_t line = 0; line < numLines; ++line)
        {
            for (size_t col = 0; col < wrappedColumns.size(); ++col)
            {
                auto& cellLines = wrappedColumns.at(col);
                const auto& text = line < cellLines.size() ? cellLines[line] : std::string("");
                stringBuilder << fmt::format("{:<{}s}", text, columnWidths.at(col));
                if (col < wrappedColumns.size() - 1)
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
