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

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/ResultComparison.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
using Type = DataType::Type;

Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::vector<std::pair<std::string, DataType>>& fields)
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        fields
        | std::views::transform(
            [](const auto& field)
            {
                const auto& [name, dataType] = field;
                return UnqualifiedUnboundField{Identifier::parse(name), dataType};
            })
        | std::ranges::to<std::vector>()};
}

Schema<UnqualifiedUnboundField, Ordered> createSchema(const std::vector<std::pair<std::string, Type>>& fields)
{
    return createSchema(
        fields
        | std::views::transform(
            [](const auto& field)
            {
                const auto& [name, type] = field;
                return std::pair{name, DataTypeProvider::provideDataType(type)};
            })
        | std::ranges::to<std::vector>());
}

std::string getReport(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows)
{
    const auto verdict = compareResults(expectedSchema, expectedRows, actualSchema, actualRows);
    return verdict.has_value() ? std::string{} : verdict.error().detail;
}

std::string getRowReport(
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const std::vector<std::string>& expectedRows,
    const std::vector<std::string>& actualRows)
{
    return getReport(schema, expectedRows, schema, actualRows);
}

std::string getValueReport(const Type type, const std::string& expected, const std::string& actual)
{
    return getRowReport(createSchema({{"v", type}}), {expected}, {actual});
}

ResultDiff diffRowsOf(
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const std::vector<std::string>& expectedRows,
    const std::vector<std::string>& actualRows)
{
    return diffResults(schema, expectedRows, schema, actualRows);
}

/// Lists what the comparison decided for each row in report order, without the layout of the report.
/// Each entry is `=` matched, `-` missing, `+` unexpected, `!` wrong field count or `~` partial match, then the row's text.
std::vector<std::string> describeRows(const ResultDiff& diff)
{
    return diff.rows
        | std::views::transform(
               [](const RowDiff& row)
               {
                   return std::visit(
                       Overloaded{
                           [](const Matched& matched) { return "= " + matched.expected.text; },
                           [](const Missing& missing) { return "- " + missing.expected.text; },
                           [](const Unexpected& unexpected) { return "+ " + unexpected.actual.text; },
                           [](const WrongFieldCount& wrongFieldCount) { return "! " + wrongFieldCount.expected.text; },
                           [](const PartialMatch& partialMatch) { return "~ " + partialMatch.expected.text; }},
                       row);
               })
        | std::ranges::to<std::vector>();
}

/// Lists every expected field that matches no actual field after `-`, then every actual field that matches no expected field after `+`.
std::vector<std::string> describeSchemas(const ResultDiff& diff)
{
    std::vector<std::string> fields;
    for (const auto& [field, type] : std::views::zip(diff.expectedSchema, diff.alignment.matchedTypes))
    {
        if (not type.has_value())
        {
            fields.push_back(fmt::format("- {}", field.getFullyQualifiedName()));
        }
    }
    for (const auto& [column, field] : diff.actualSchema | views::enumerate)
    {
        if (std::ranges::contains(diff.alignment.unexpectedColumns, static_cast<size_t>(column)))
        {
            fields.push_back(fmt::format("+ {}", field.getFullyQualifiedName()));
        }
    }
    return fields;
}

using Lines = std::vector<std::string>;

std::string formatField(const std::string_view name, const std::string_view type)
{
    return fmt::format("QualifiedUnboundField: (name: {}, type: DataType(type: {} nullable: false))", name, type);
}

std::string formatMissingField(const std::string_view field)
{
    return fmt::format("- '{}' is missing from actual result schema.", field);
}

std::string formatUnexpectedField(const std::string_view field)
{
    return fmt::format("+ '{}' is unexpected field in actual result schema.", field);
}

constexpr std::string_view SchemaMismatchHeader = "Schema Mismatch\n---------------\n";
constexpr std::string_view RowMismatchHeader = "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                               "-------------------------------------------------\n";

std::string formatRowMismatch(const std::string_view rowLines)
{
    return fmt::format("{}{}", RowMismatchHeader, rowLines);
}

std::string formatSchemaMismatch(const std::vector<std::string>& schemaLines)
{
    return fmt::format("{}{}\n\nAll Results match", SchemaMismatchHeader, fmt::join(schemaLines, "\n"));
}

std::string formatSchemaAndRowMismatch(const std::vector<std::string>& schemaLines, const std::string_view rowLines)
{
    return fmt::format("{}{}\n\n{}{}", SchemaMismatchHeader, fmt::join(schemaLines, "\n"), RowMismatchHeader, rowLines);
}

class ResultComparisonTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ResultComparisonTest.log", LogLevel::LOG_DEBUG); }
};
}

TEST_F(ResultComparisonTest, TwoEmptyResultsMatch)
{
    EXPECT_EQ(getRowReport(createSchema({{"a", Type::UINT64}}), {}, {}), "");
}

TEST_F(ResultComparisonTest, ListsEveryRowWhenOneSideIsEmpty)
{
    const auto schema = createSchema({{"a", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"3", "12"}, {})), Lines({"- 12", "- 3"}));
    EXPECT_EQ(describeRows(diffRowsOf(schema, {}, {"3", "12"})), Lines({"+ 12", "+ 3"}));
}

TEST_F(ResultComparisonTest, IgnoresRowOrder)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"3 1", "1 2", "2 3"}, {"2 3", "3 1", "1 2"}), "");
}

TEST_F(ResultComparisonTest, ComparesRowsAsAMultiset)
{
    const auto schema = createSchema({{"a", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1", "1"}, {"1"})), Lines({"= 1", "- 1"}));
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1"}, {"1", "1"})), Lines({"= 1", "+ 1"}));
}

TEST_F(ResultComparisonTest, SortsRowsAsTextInTheReport)
{
    EXPECT_EQ(getRowReport(createSchema({{"a", Type::UINT64}}), {"9", "10"}, {"9", "11"}), formatRowMismatch("10 | __\n__ | 11\n9 | 9"));
}

TEST_F(ResultComparisonTest, SkipsTheSmallerRowOfAMismatchingPair)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1 2", "1 1"}, {"1 1", "1 3"})), Lines({"= 1 1", "- 1 2", "+ 1 3"}));
}

TEST_F(ResultComparisonTest, TreatsCommasAndRunsOfSpacesAsOneSeparator)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1,2,3"}, {"1 2 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1, 2,3"}, {"1 2 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1  2   3"}, {"1,2,3"}), "");
}

TEST_F(ResultComparisonTest, CountsALeadingOrTrailingSpaceAsAnEmptyField)
{
    const auto schema = createSchema({{"a", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {" 1"}, {"1"})), Lines({"!  1", "+ 1"}));
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1"}, {"1 "})), Lines({"~ 1"}));
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1"}, {" 1"})), Lines({"+  1", "- 1"}));
}

TEST_F(ResultComparisonTest, ReadsAnEmptyCommaSeparatedColumnAsNull)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1,,3"}, {"1 NULL 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1,2,"}, {"1 2 null"}), "");
    EXPECT_EQ(getRowReport(schema, {",2,3"}, {"NULL,2,3"}), "");
    EXPECT_EQ(getRowReport(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}), {","}, {"NULL NULL"}), "");
}

TEST_F(ResultComparisonTest, DoesNotReadAnEmptySpaceSeparatedColumnAsNull)
{
    EXPECT_EQ(
        describeRows(diffRowsOf(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}}), {"1  3"}, {"1 NULL 3"})),
        Lines({"! 1 3", "+ 1 NULL 3"}));
}

TEST_F(ResultComparisonTest, MatchesNullOnlyWithNullInAnyCase)
{
    EXPECT_TRUE(compareValue(Type::UINT64, "NULL", "null"));
    EXPECT_TRUE(compareValue(Type::FLOAT64, "null", "NULL"));
    EXPECT_TRUE(compareValue(Type::BOOLEAN, "NULL", "nUlL"));
    EXPECT_FALSE(compareValue(Type::UINT64, "NULL", "0"));
    EXPECT_FALSE(compareValue(Type::UINT64, "0", "NULL"));
}

TEST_F(ResultComparisonTest, SortsNullByItsTextBeforeMatchingItInAnyCase)
{
    const auto schema = createSchema({{"s", Type::VARSIZED}, {"n", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {",1", "a,2"}, {"null,1", "a,2"})), Lines({"- NULL 1", "= a 2", "+ null 1"}));
}

TEST_F(ResultComparisonTest, ComparesIntegersAsText)
{
    EXPECT_FALSE(compareValue(Type::UINT64, "01", "1"));
    EXPECT_FALSE(compareValue(Type::INT8, "+1", "1"));
}

TEST_F(ResultComparisonTest, ComparesFloatsWithinARelativeEpsilonOfTheSum)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1.0", "1.00002"));
    EXPECT_FALSE(compareValue(Type::FLOAT64, "1.0", "1.000021"));
    EXPECT_TRUE(compareValue(Type::FLOAT64, "-1.0", "-1.00002"));
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1000000", "1000020"));
    EXPECT_FALSE(compareValue(Type::FLOAT64, "0.000001", "-0.000001"));
    EXPECT_TRUE(compareValue(Type::FLOAT32, "1.0", "1.00002"));
}

TEST_F(ResultComparisonTest, ComparesFloatsWhoseSumOverflowsAgainstTheLargestDouble)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1e308", "1.00001e308"));
    EXPECT_FALSE(compareValue(Type::FLOAT64, "1e308", "1.5e308"));
}

TEST_F(ResultComparisonTest, ComparesFloatsByValueWhenTheTextDiffers)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1", "1.0"));
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1e1", "10.0"));
    EXPECT_TRUE(compareValue(Type::FLOAT64, "inf", "INF"));
}

TEST_F(ResultComparisonTest, ComparesAFloatWithZeroAlmostExactly)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "-0.0", "0.0"));
    EXPECT_FALSE(compareValue(Type::FLOAT64, "0", "0.0000001"));
}

TEST_F(ResultComparisonTest, ComparesTwoSubnormalFloatsAgainstAnAbsoluteBound)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "0x1p-1070", "0x1p-1069"));
    EXPECT_FALSE(compareValue(Type::FLOAT64, "0x1p-1030", "0x1p-1029"));
}

TEST_F(ResultComparisonTest, SortsSignedZerosApartBeforeMatchingThem)
{
    EXPECT_EQ(
        describeRows(diffRowsOf(createSchema({{"f", Type::FLOAT64}}), {"-0.0", "-1.0"}, {"0.0", "-1.0"})),
        Lines({"- -0.0", "= -1.0", "+ 0.0"}));
}

TEST_F(ResultComparisonTest, ParsesOnlyTheNumericPrefixOfAFloat)
{
    EXPECT_TRUE(compareValue(Type::FLOAT64, "1.0abc", "1.0"));
}

TEST_F(ResultComparisonTest, NeverMatchesTwoNansThatDifferAsText)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "nan", "nan"), "");
    EXPECT_FALSE(compareValue(Type::FLOAT64, "nan", "NaN"));
}

TEST_F(ResultComparisonTest, ThrowsOnAFloatWithoutANumericPrefix)
{
    const auto schema = createSchema({{"f", Type::FLOAT64}});
    EXPECT_THROW(std::ignore = compareValue(Type::FLOAT64, "abc", "1.0"), std::invalid_argument);
    EXPECT_THROW(std::ignore = compareValue(Type::FLOAT64, "1.0", "abc"), std::invalid_argument);
    EXPECT_EQ(getRowReport(schema, {"abc"}, {"abc"}), "");
}

TEST_F(ResultComparisonTest, ThrowsOnADecimalFloatOutsideTheNormalRange)
{
    const auto schema = createSchema({{"f", Type::FLOAT64}});
    EXPECT_THROW(std::ignore = compareValue(Type::FLOAT64, "1e-310", "2e-310"), std::out_of_range);
    EXPECT_THROW(std::ignore = compareValue(Type::FLOAT64, "1.0", "1e309"), std::out_of_range);
    EXPECT_EQ(getRowReport(schema, {"1e-310"}, {"1e-310"}), "");
}

TEST_F(ResultComparisonTest, ReadsBooleansAsWordsOrIntegers)
{
    EXPECT_TRUE(compareValue(Type::BOOLEAN, "False", "0"));
    EXPECT_TRUE(compareValue(Type::BOOLEAN, "2", "1"));
    EXPECT_TRUE(compareValue(Type::BOOLEAN, "-1", "true"));
    EXPECT_FALSE(compareValue(Type::BOOLEAN, "true", "0"));
}

TEST_F(ResultComparisonTest, StopsOnABooleanThatIsNeitherWordNorInteger)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto schema = createSchema({{"b", Type::BOOLEAN}});
    EXPECT_EQ(getRowReport(schema, {"yes"}, {"yes"}), "");
    EXPECT_DEATH_DEBUG(std::ignore = compareValue(Type::BOOLEAN, "yes", "true"), "");
}

TEST_F(ResultComparisonTest, ComparesTextExactly)
{
    EXPECT_FALSE(compareValue(Type::VARSIZED, "abc", "ABC"));
    EXPECT_FALSE(compareValue(Type::CHAR, "a", "A"));
}

TEST_F(ResultComparisonTest, SplitsTextOnSpacesAndCommas)
{
    EXPECT_EQ(describeRows(diffRowsOf(createSchema({{"s", Type::VARSIZED}}), {"a b,c"}, {"a c"})), Lines({"! a b c", "+ a c"}));
}

TEST_F(ResultComparisonTest, ThrowsOnAnUndefinedFieldWhoseTextDiffers)
{
    const auto schema
        = createSchema(std::vector<std::pair<std::string, DataType>>{{"u", DataType{Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE}}});
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1"}), "");
    ASSERT_EXCEPTION_ERRORCODE(std::ignore = compareValue(Type::UNDEFINED, "1", "2"), ErrorCode::UnknownDataType);
}

TEST_F(ResultComparisonTest, ListsAPairThatMatchesByValueWithBothTexts)
{
    EXPECT_EQ(getRowReport(createSchema({{"f", Type::FLOAT64}}), {"1.0", "2"}, {"1", "3"}), formatRowMismatch("1.0 | 1\n2 | _\n_ | 3"));
}

TEST_F(ResultComparisonTest, ChecksTheFieldCountOfAnExpectedRowAgainstTheExpectedSchema)
{
    const auto diff
        = diffResults(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1"}, createSchema({{"a", Type::UINT64}}), {"2"});
    EXPECT_EQ(describeSchemas(diff), Lines({"- B"}));
    EXPECT_EQ(describeRows(diff), Lines({"! 1", "+ 2"}));
    EXPECT_EQ(std::get<WrongFieldCount>(diff.rows.front()).fieldCount, 2UZ);
}

TEST_F(ResultComparisonTest, ChecksTheFieldCountOnlyOfAPairedRowWhoseTextDiffers)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1"}, {})), Lines({"- 1"}));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1"}), "");
}

TEST_F(ResultComparisonTest, SortsTheMissingValueOfAShortRowAsEmpty)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(describeRows(diffRowsOf(schema, {"1 2", "1"}, {"1 2"})), Lines({"! 1", "= 1 2"}));
    EXPECT_EQ(describeRows(diffRowsOf(schema, {}, {"1 2", "1"})), Lines({"+ 1", "+ 1 2"}));
    EXPECT_EQ(getRowReport(schema, {"1", "1 2"}, {"1 2", "1"}), "");
}

TEST_F(ResultComparisonTest, ThrowsOnAnActualRowShorterThanTheMatchedFields)
{
    EXPECT_THROW(getRowReport(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1 2"}, {"1"}), std::out_of_range);
}

TEST_F(ResultComparisonTest, ThrowsOnAnActualRowShorterThanTheUnexpectedFields)
{
    EXPECT_THROW(
        getReport(
            createSchema({{"a", Type::UINT64}}),
            {"1"},
            createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}}),
            {"1 2"}),
        std::out_of_range);
}

TEST_F(ResultComparisonTest, ReadsAnEmptyRowAsNoFields)
{
    EXPECT_EQ(describeRows(diffRowsOf(createSchema({{"a", Type::UINT64}}), {}, {""})), Lines({"+ "}));
    EXPECT_EQ(describeRows(diffRowsOf(createSchema({{"a", Type::UINT64}}), {""}, {"1"})), Lines({"! ", "+ 1"}));
}

TEST_F(ResultComparisonTest, ComparesNamesCaseSensitivelyOnlyWhenTheExpectedNameIsQuoted)
{
    EXPECT_EQ(getReport(createSchema({{"Id", Type::UINT64}}), {}, createSchema({{"\"Id\"", Type::UINT64}}), {}), "");
    EXPECT_EQ(getReport(createSchema({{"\"ID\"", Type::UINT64}}), {}, createSchema({{"id", Type::UINT64}}), {}), "");
    EXPECT_EQ(
        describeSchemas(diffResults(createSchema({{"\"Id\"", Type::UINT64}}), {}, createSchema({{"Id", Type::UINT64}}), {})),
        Lines({"- Id", "+ ID"}));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseTypeDiffers)
{
    const auto diff = diffResults(createSchema({{"id", Type::UINT64}}), {"1"}, createSchema({{"id", Type::INT64}}), {"2"});
    EXPECT_EQ(describeSchemas(diff), Lines({"- ID", "+ ID"}));
    EXPECT_EQ(describeRows(diff), Lines({"~ 1"}));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseNullabilityDiffers)
{
    const auto diff = diffResults(
        createSchema({{"id", Type::UINT64}}),
        {"1"},
        createSchema(
            std::vector<std::pair<std::string, DataType>>{
                {"id", DataTypeProvider::provideDataType(Type::UINT64, DataType::NULLABLE::IS_NULLABLE)}}),
        {"1"});
    EXPECT_EQ(describeSchemas(diff), Lines({"- ID", "+ ID"}));
    EXPECT_TRUE(diff.rowsMatch());
}

TEST_F(ResultComparisonTest, ReportsTheFieldCountOfAnExpectedRow)
{
    EXPECT_EQ(
        getRowReport(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1", "1 2 3"}, {"1 2"}),
        formatRowMismatch(
            "1 | Not enough expected fields (expected sink schema has: 2, but got 1)\n"
            "1 2 3 | Too many expected fields (expected sink schema has: 2, but got 3)\n"
            "___ | 1 2"));
}

TEST_F(ResultComparisonTest, ReportsASchemaMismatchAboveMatchingRows)
{
    const auto expectedField = formatField("ID", "UINT64");
    const auto actualField = formatField("ID", "INT64");
    EXPECT_EQ(
        getReport(createSchema({{"id", Type::UINT64}}), {"1"}, createSchema({{"id", Type::INT64}}), {"1"}),
        formatSchemaMismatch(
            {fmt::format("{} != {}", expectedField, actualField), formatMissingField(expectedField), formatUnexpectedField(actualField)}));
}

TEST_F(ResultComparisonTest, FillsTheColumnsOfAnUnexpectedFieldWithPlaceholders)
{
    const auto id = formatField("ID", "UINT64");
    const auto extra = formatField("EXTRA", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"id", Type::UINT64}}), {"1"}, createSchema({{"id", Type::UINT64}, {"extra", Type::UINT64}}), {"1,2"}),
        formatSchemaAndRowMismatch({fmt::format("{} != {}, {}", id, id, extra), formatUnexpectedField(extra)}, "1 _ | 1 2"));
}

TEST_F(ResultComparisonTest, FillsTheMatchedColumnsOfAnAlignedRowWithTheActualText)
{
    const auto value = formatField("V", "FLOAT64");
    const auto extra = formatField("EXTRA", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"v", Type::FLOAT64}}), {"1.0"}, createSchema({{"v", Type::FLOAT64}, {"extra", Type::UINT64}}), {"1,2"}),
        formatSchemaAndRowMismatch({fmt::format("{} != {}, {}", value, value, extra), formatUnexpectedField(extra)}, "1.0 _ | 1 2"));
}

TEST_F(ResultComparisonTest, FillsTheColumnsOfAMissingFieldWithPlaceholders)
{
    const auto id = formatField("ID", "UINT64");
    const auto value = formatField("VALUE", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"id", Type::UINT64}, {"value", Type::UINT64}}), {"1,2"}, createSchema({{"id", Type::UINT64}}), {"1"}),
        formatSchemaAndRowMismatch({fmt::format("{}, {} != {}", id, value, id), formatMissingField(value)}, "1 2 | 1 _"));
}

TEST_F(ResultComparisonTest, ReportsSwappedFieldsAsMissingAndUnexpected)
{
    const auto diff = diffResults(
        createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}),
        {"1 2"},
        createSchema({{"b", Type::UINT64}, {"a", Type::UINT64}}),
        {"2 1"});
    EXPECT_EQ(describeSchemas(diff), Lines({"- A", "- B", "+ B", "+ A"}));
    EXPECT_EQ(describeRows(diff), Lines({"~ 1 2"}));
}

TEST_F(ResultComparisonTest, KeepsTheInputOrderWhenNoFieldMatches)
{
    const auto diff = diffResults(createSchema({{"a", Type::UINT64}}), {"2", "1"}, createSchema({{"b", Type::UINT64}}), {"1", "2"});
    EXPECT_EQ(describeSchemas(diff), Lines({"- A", "+ B"}));
    EXPECT_EQ(describeRows(diff), Lines({"~ 2", "~ 1"}));
}

}
