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

#include <ResultChecker/ResultComparison.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
enum class Advance : uint8_t
{
    Expected,
    Actual,
    Both
};

struct RowStep
{
    Advance advance;
    RowDiff diff;
};

/// The expected side is the schema that the test declared for the sink, and the actual side is the header that the sink wrote.
/// A name that the test wrote unquoted has no case of its own, so it matches the written name whatever case that has.
bool compareField(const UnqualifiedUnboundField& expectedField, const UnqualifiedUnboundField& actualField)
{
    if (expectedField.getDataType() != actualField.getDataType())
    {
        return false;
    }
    const auto& expectedIdentifier = *expectedField.getFullyQualifiedName().begin();
    const auto& actualIdentifier = *actualField.getFullyQualifiedName().begin();
    return expectedIdentifier.isCaseSensitive()
        ? expectedIdentifier == actualIdentifier
        : expectedIdentifier.asCanonicalString() == toUpperCase(actualIdentifier.asCanonicalString());
}

/// Fields pair up by position, because the column order is part of the query's semantics.
/// A column at the wrong position reports as missing on one side and unexpected on the other.
SchemaAlignment
alignSchemas(const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema, const Schema<UnqualifiedUnboundField, Ordered>& actualSchema)
{
    SchemaAlignment alignment;
    for (const auto column : std::views::iota(0UZ, std::max(expectedSchema.size(), actualSchema.size())))
    {
        /// A schema returns no field for an index past its end.
        /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        const auto expectedField = expectedSchema[column];
        const auto actualField = actualSchema[column];
        /// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
        const auto matched = expectedField.has_value() and actualField.has_value() and compareField(*expectedField, *actualField);
        if (expectedField.has_value())
        {
            alignment.matchedTypes.push_back(matched ? std::optional{expectedField->getDataType().type} : std::nullopt);
        }
        if (actualField.has_value() and not matched)
        {
            alignment.unexpectedColumns.push_back(column);
        }
    }
    return alignment;
}

std::vector<size_t> getMatchedColumns(const SchemaAlignment& alignment)
{
    return std::views::iota(0UZ, alignment.matchedTypes.size())
        | std::views::filter([&alignment](const size_t column) { return alignment.matchedTypes.at(column).has_value(); })
        | std::ranges::to<std::vector>();
}

std::vector<std::string> formatSchemaLines(const ResultDiff& diff)
{
    std::vector<std::string> lines;
    for (const auto& [expectedField, type] : std::views::zip(diff.expectedSchema, diff.alignment.matchedTypes))
    {
        if (not type.has_value())
        {
            lines.push_back(fmt::format("- '{}' is missing from actual result schema.", expectedField));
        }
    }
    for (const auto& [column, actualField] : diff.actualSchema | views::enumerate)
    {
        if (std::ranges::contains(diff.alignment.unexpectedColumns, static_cast<size_t>(column)))
        {
            lines.push_back(fmt::format("+ '{}' is unexpected field in actual result schema.", actualField));
        }
    }
    if (not lines.empty())
    {
        lines.insert(lines.begin(), fmt::format("{} != {}", fmt::join(diff.expectedSchema, ", "), fmt::join(diff.actualSchema, ", ")));
    }
    return lines;
}

/// An empty column becomes NULL, so it has a value to compare.
/// Both sides may separate fields with commas, so both drop them.
/// Double spaces go too, because a single space separates the fields.
template <typename RowType>
RowType normalizeRow(const std::string& row)
{
    auto text = row | std::views::split(',')
        | std::views::transform([](const auto& part) { return part.empty() ? std::string_view{"NULL"} : std::string_view{part}; })
        | std::views::join_with(' ') | std::ranges::to<std::string>();
    removeDoubleSpaces(text);
    auto values = text | std::views::split(' ') | std::ranges::to<std::vector<std::string>>();
    return RowType{.text = std::move(text), .values = std::move(values)};
}

template <typename RowType>
std::vector<RowType> sortRows(std::vector<RowType> rows, const std::vector<size_t>& sortColumns)
{
    const auto getSortValue = [](const RowType& row, const size_t column)
    { return column < row.values.size() ? std::string_view{row.values.at(column)} : std::string_view{}; };
    std::ranges::sort(
        rows,
        [&sortColumns, &getSortValue](const RowType& lhs, const RowType& rhs)
        {
            return std::ranges::lexicographical_compare(
                sortColumns,
                sortColumns,
                std::ranges::less{},
                [&lhs, &getSortValue](const size_t column) { return getSortValue(lhs, column); },
                [&rhs, &getSortValue](const size_t column) { return getSortValue(rhs, column); });
        });
    return rows;
}

template <typename RowType>
std::vector<RowType> prepareRows(const std::vector<std::string>& rows, const std::vector<size_t>& sortColumns)
{
    return sortRows(rows | std::views::transform(normalizeRow<RowType>) | std::ranges::to<std::vector>(), sortColumns);
}

/// Reads a boolean written either as an integer, where anything but 0 is true, or as `true` or `false`.
bool parseBool(const std::string& value)
{
    if (const auto lower = toLowerCase(value); lower == "true" or lower == "false")
    {
        return lower == "true";
    }
    const auto number = from_chars<int>(value);
    INVARIANT(number.has_value(), "Cannot convert '{}' to bool", value);
    return static_cast<bool>(number.value());
}

bool compareFloat(const std::string& expected, const std::string& actual)
{
    static constexpr auto Epsilon = 1e-5;
    const auto expectedNumber = std::stod(expected);
    const auto actualNumber = std::stod(actual);
    if (expectedNumber == actualNumber)
    {
        return true;
    }
    const auto difference = std::abs(expectedNumber - actualNumber);
    const auto magnitude = std::abs(expectedNumber) + std::abs(actualNumber);
    if (magnitude < std::numeric_limits<double>::min())
    {
        return difference < (Epsilon * std::numeric_limits<double>::min());
    }
    const auto relativeError = difference / std::min(magnitude, std::numeric_limits<double>::max());
    const auto withinEpsilon = relativeError < Epsilon;
    if (not withinEpsilon)
    {
        NES_TRACE(
            "Relative error {} is greater than allowed error {} for values {} and {}",
            relativeError,
            Epsilon,
            expectedNumber,
            actualNumber);
    }
    return withinEpsilon;
}

}

bool compareValue(const DataType::Type type, const std::string& expected, const std::string& actual)
{
    const auto expectedIsNull = toLowerCase(expected) == "null";
    const auto actualIsNull = toLowerCase(actual) == "null";
    if (expectedIsNull or actualIsNull)
    {
        return expectedIsNull and actualIsNull;
    }

    switch (type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
            return expected == actual;
        case DataType::Type::BOOLEAN: {
            const auto expectedBool = parseBool(expected);
            const auto actualBool = parseBool(actual);
            return expectedBool == actualBool;
        }
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return compareFloat(expected, actual);
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting UNDEFINED in result check comparison");
    }
    std::unreachable();
}

namespace
{
std::string formatPair(const std::string_view expected, const std::string_view actual)
{
    return fmt::format("{} | {}", expected, actual);
}

std::string formatMissingRow(const ExpectedRow& expected)
{
    return formatPair(expected.text, std::string(expected.text.size(), '_'));
}

std::string formatUnexpectedRow(const ActualRow& actual)
{
    return formatPair(std::string(actual.text.size(), '_'), actual.text);
}

std::string formatFieldCountMismatch(const ExpectedRow& expected, const size_t columnCount)
{
    return formatPair(
        expected.text,
        fmt::format(
            "{} (expected sink schema has: {}, but got {})",
            expected.values.size() < columnCount ? "Not enough expected fields" : "Too many expected fields",
            columnCount,
            expected.values.size()));
}

std::string formatAlignedRow(const SchemaAlignment& alignment, const ExpectedRow& expected, const ActualRow& actual)
{
    std::vector<std::string_view> expectedValues;
    std::vector<std::string_view> actualValues;
    for (const auto& [column, type] : alignment.matchedTypes | views::enumerate)
    {
        expectedValues.emplace_back(expected.values.at(column));
        actualValues.emplace_back(type.has_value() ? std::string_view{actual.values.at(column)} : "_");
    }
    for (const auto column : alignment.unexpectedColumns)
    {
        expectedValues.emplace_back("_");
        actualValues.emplace_back(actual.values.at(column));
    }
    return formatPair(fmt::to_string(fmt::join(expectedValues, " ")), fmt::to_string(fmt::join(actualValues, " ")));
}

std::string formatRow(const SchemaAlignment& alignment, const RowDiff& row)
{
    return std::visit(
        Overloaded{
            [](const Matched& matched) { return formatPair(matched.expected.text, matched.actual.text); },
            [](const Missing& missing) { return formatMissingRow(missing.expected); },
            [](const Unexpected& unexpected) { return formatUnexpectedRow(unexpected.actual); },
            [](const WrongFieldCount& wrongFieldCount)
            { return formatFieldCountMismatch(wrongFieldCount.expected, wrongFieldCount.fieldCount); },
            [&alignment](const PartialMatch& partialMatch)
            { return formatAlignedRow(alignment, partialMatch.expected, partialMatch.actual); }},
        row);
}

/// Equal text matches before the field count is checked, so an expected row with the wrong field count still matches identical output.
RowStep compareRow(const SchemaAlignment& alignment, const ExpectedRow& expected, const ActualRow& actual)
{
    if (expected.text == actual.text)
    {
        return RowStep{.advance = Advance::Both, .diff = Matched{.expected = expected, .actual = actual}};
    }
    if (const auto columnCount = alignment.matchedTypes.size(); expected.values.size() != columnCount)
    {
        return RowStep{.advance = Advance::Expected, .diff = WrongFieldCount{.expected = expected, .fieldCount = columnCount}};
    }
    if (const auto valuesMatch = std::ranges::all_of(
            alignment.matchedTypes | views::enumerate,
            [&expected, &actual](const auto& matchedType)
            {
                const auto& [column, type] = matchedType;
                return not type.has_value() or compareValue(*type, expected.values.at(column), actual.values.at(column));
            });
        not valuesMatch)
    {
        return expected.text < actual.text ? RowStep{.advance = Advance::Expected, .diff = Missing{.expected = expected}}
                                           : RowStep{.advance = Advance::Actual, .diff = Unexpected{.actual = actual}};
    }
    if (std::ranges::all_of(alignment.matchedTypes, [](const auto& type) { return type.has_value(); })
        and expected.values.size() == actual.values.size())
    {
        return RowStep{.advance = Advance::Both, .diff = Matched{.expected = expected, .actual = actual}};
    }
    return RowStep{.advance = Advance::Both, .diff = PartialMatch{.expected = expected, .actual = actual}};
}

std::vector<RowDiff>
diffRows(const SchemaAlignment& alignment, const std::vector<ExpectedRow>& expectedRows, const std::vector<ActualRow>& actualRows)
{
    std::vector<RowDiff> rows;
    auto expected = std::ranges::subrange{expectedRows};
    auto actual = std::ranges::subrange{actualRows};
    while (not expected.empty() and not actual.empty())
    {
        auto [advance, diff] = compareRow(alignment, expected.front(), actual.front());
        rows.push_back(std::move(diff));
        if (advance != Advance::Actual)
        {
            expected.advance(1);
        }
        if (advance != Advance::Expected)
        {
            actual.advance(1);
        }
    }
    rows.append_range(expected | std::views::transform([](const ExpectedRow& row) { return RowDiff{Missing{.expected = row}}; }));
    rows.append_range(actual | std::views::transform([](const ActualRow& row) { return RowDiff{Unexpected{.actual = row}}; }));
    return rows;
}
}

bool ResultDiff::schemasMatch() const
{
    return alignment.unexpectedColumns.empty()
        and std::ranges::all_of(alignment.matchedTypes, [](const auto& type) { return type.has_value(); });
}

bool ResultDiff::rowsMatch() const
{
    return std::ranges::all_of(rows, [](const RowDiff& row) { return std::holds_alternative<Matched>(row); });
}

ResultDiff diffResults(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows)
{
    auto alignment = alignSchemas(expectedSchema, actualSchema);
    const auto sortColumns = getMatchedColumns(alignment);
    auto rows = diffRows(alignment, prepareRows<ExpectedRow>(expectedRows, sortColumns), prepareRows<ActualRow>(actualRows, sortColumns));
    return ResultDiff{
        .expectedSchema = expectedSchema, .actualSchema = actualSchema, .alignment = std::move(alignment), .rows = std::move(rows)};
}

std::string renderReport(const ResultDiff& diff)
{
    static constexpr std::string_view SchemaMismatchHeader = "Schema Mismatch\n"
                                                             "---------------";
    static constexpr std::string_view ResultMismatchHeader = "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                                             "-------------------------------------------------";
    const auto schemaBlock
        = diff.schemasMatch() ? std::string{} : fmt::format("{}\n{}\n\n", SchemaMismatchHeader, fmt::join(formatSchemaLines(diff), "\n"));
    if (diff.rowsMatch())
    {
        return schemaBlock + "All Results match";
    }
    const auto rowLines = diff.rows | std::views::transform([&diff](const RowDiff& row) { return formatRow(diff.alignment, row); })
        | std::ranges::to<std::vector>();
    return schemaBlock + fmt::format("{}\n{}", ResultMismatchHeader, fmt::join(rowLines, "\n"));
}

Verdict compareResults(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows)
{
    const auto diff = diffResults(expectedSchema, expectedRows, actualSchema, actualRows);
    if (diff.schemasMatch() and diff.rowsMatch())
    {
        return Success{};
    }
    return std::unexpected{Mismatch{renderReport(diff)}};
}

}
