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
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
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
    EXPECT_EQ(getRowReport(schema, {"3", "12"}, {}), formatRowMismatch("12 | __\n3 | _"));
    EXPECT_EQ(getRowReport(schema, {}, {"3", "12"}), formatRowMismatch("__ | 12\n_ | 3"));
}

TEST_F(ResultComparisonTest, IgnoresRowOrder)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"3 1", "1 2", "2 3"}, {"2 3", "3 1", "1 2"}), "");
}

TEST_F(ResultComparisonTest, ComparesRowsAsAMultiset)
{
    const auto schema = createSchema({{"a", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1", "1"}, {"1"}), formatRowMismatch("1 | 1\n1 | _"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1", "1"}), formatRowMismatch("1 | 1\n_ | 1"));
}

TEST_F(ResultComparisonTest, SortsRowsAsTextInTheReport)
{
    EXPECT_EQ(getRowReport(createSchema({{"a", Type::UINT64}}), {"9", "10"}, {"9", "11"}), formatRowMismatch("10 | __\n__ | 11\n9 | 9"));
}

TEST_F(ResultComparisonTest, SkipsTheSmallerRowOfAMismatchingPair)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1 2", "1 1"}, {"1 1", "1 3"}), formatRowMismatch("1 1 | 1 1\n1 2 | ___\n___ | 1 3"));
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
    EXPECT_EQ(
        getRowReport(schema, {" 1"}, {"1"}),
        formatRowMismatch(" 1 | Too many expected fields (expected sink schema has: 1, but got 2)\n_ | 1"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1 "}), formatRowMismatch("1 | 1"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {" 1"}), formatRowMismatch("__ |  1\n1 | _"));
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
        getRowReport(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}}), {"1  3"}, {"1 NULL 3"}),
        formatRowMismatch("1 3 | Not enough expected fields (expected sink schema has: 3, but got 2)\n________ | 1 NULL 3"));
}

TEST_F(ResultComparisonTest, MatchesNullOnlyWithNullInAnyCase)
{
    EXPECT_EQ(getValueReport(Type::UINT64, "NULL", "null"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "null", "NULL"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "NULL", "nUlL"), "");
    EXPECT_EQ(getValueReport(Type::UINT64, "NULL", "0"), formatRowMismatch("_ | 0\nNULL | ____"));
    EXPECT_EQ(getValueReport(Type::UINT64, "0", "NULL"), formatRowMismatch("0 | _\n____ | NULL"));
}

TEST_F(ResultComparisonTest, SortsNullByItsTextBeforeMatchingItInAnyCase)
{
    const auto schema = createSchema({{"s", Type::VARSIZED}, {"n", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {",1", "a,2"}, {"null,1", "a,2"}), formatRowMismatch("NULL 1 | ______\na 2 | a 2\n______ | null 1"));
}

TEST_F(ResultComparisonTest, ComparesIntegersAsText)
{
    EXPECT_EQ(getValueReport(Type::UINT64, "01", "1"), formatRowMismatch("01 | __\n_ | 1"));
    EXPECT_EQ(getValueReport(Type::INT8, "+1", "1"), formatRowMismatch("+1 | __\n_ | 1"));
}

TEST_F(ResultComparisonTest, ComparesFloatsWithinARelativeEpsilonOfTheSum)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.00002"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.000021"), formatRowMismatch("1.0 | ___\n________ | 1.000021"));
    EXPECT_EQ(getValueReport(Type::FLOAT64, "-1.0", "-1.00002"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1000000", "1000020"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0.000001", "-0.000001"), formatRowMismatch("_________ | -0.000001\n0.000001 | ________"));
    EXPECT_EQ(getValueReport(Type::FLOAT32, "1.0", "1.00002"), "");
}

TEST_F(ResultComparisonTest, ComparesFloatsWhoseSumOverflowsAgainstTheLargestDouble)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1e308", "1.00001e308"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1e308", "1.5e308"), formatRowMismatch("_______ | 1.5e308\n1e308 | _____"));
}

TEST_F(ResultComparisonTest, ComparesFloatsByValueWhenTheTextDiffers)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1", "1.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1e1", "10.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "inf", "INF"), "");
}

TEST_F(ResultComparisonTest, ComparesAFloatWithZeroAlmostExactly)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "-0.0", "0.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0", "0.0000001"), formatRowMismatch("0 | _\n_________ | 0.0000001"));
}

TEST_F(ResultComparisonTest, ComparesTwoSubnormalFloatsAgainstAnAbsoluteBound)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0x1p-1070", "0x1p-1069"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0x1p-1030", "0x1p-1029"), formatRowMismatch("_________ | 0x1p-1029\n0x1p-1030 | _________"));
}

TEST_F(ResultComparisonTest, SortsSignedZerosApartBeforeMatchingThem)
{
    EXPECT_EQ(
        getRowReport(createSchema({{"f", Type::FLOAT64}}), {"-0.0", "-1.0"}, {"0.0", "-1.0"}),
        formatRowMismatch("-0.0 | ____\n-1.0 | -1.0\n___ | 0.0"));
}

TEST_F(ResultComparisonTest, ParsesOnlyTheNumericPrefixOfAFloat)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0abc", "1.0"), "");
}

TEST_F(ResultComparisonTest, NeverMatchesTwoNansThatDifferAsText)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "nan", "nan"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "nan", "NaN"), formatRowMismatch("___ | NaN\nnan | ___"));
}

TEST_F(ResultComparisonTest, ThrowsOnAFloatWithoutANumericPrefix)
{
    const auto schema = createSchema({{"f", Type::FLOAT64}});
    EXPECT_THROW(getRowReport(schema, {"abc"}, {"1.0"}), std::invalid_argument);
    EXPECT_THROW(getRowReport(schema, {"1.0"}, {"abc"}), std::invalid_argument);
    EXPECT_EQ(getRowReport(schema, {"abc"}, {"abc"}), "");
}

TEST_F(ResultComparisonTest, ThrowsOnADecimalFloatOutsideTheNormalRange)
{
    const auto schema = createSchema({{"f", Type::FLOAT64}});
    EXPECT_THROW(getRowReport(schema, {"1e-310"}, {"2e-310"}), std::out_of_range);
    EXPECT_THROW(getRowReport(schema, {"1.0"}, {"1e309"}), std::out_of_range);
    EXPECT_EQ(getRowReport(schema, {"1e-310"}, {"1e-310"}), "");
}

TEST_F(ResultComparisonTest, ReadsBooleansAsWordsOrIntegers)
{
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "False", "0"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "2", "1"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "-1", "true"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "true", "0"), formatRowMismatch("_ | 0\ntrue | ____"));
}

TEST_F(ResultComparisonTest, StopsOnABooleanThatIsNeitherWordNorInteger)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto schema = createSchema({{"b", Type::BOOLEAN}});
    EXPECT_EQ(getRowReport(schema, {"yes"}, {"yes"}), "");
    EXPECT_DEATH_DEBUG(getRowReport(schema, {"yes"}, {"true"}), "");
}

TEST_F(ResultComparisonTest, ComparesTextExactly)
{
    EXPECT_EQ(getValueReport(Type::VARSIZED, "abc", "ABC"), formatRowMismatch("___ | ABC\nabc | ___"));
    EXPECT_EQ(getValueReport(Type::CHAR, "a", "A"), formatRowMismatch("_ | A\na | _"));
}

TEST_F(ResultComparisonTest, SplitsTextOnSpacesAndCommas)
{
    EXPECT_EQ(
        getRowReport(createSchema({{"s", Type::VARSIZED}}), {"a b,c"}, {"a c"}),
        formatRowMismatch("a b c | Too many expected fields (expected sink schema has: 1, but got 3)\n___ | a c"));
}

TEST_F(ResultComparisonTest, ThrowsOnAnUndefinedFieldWhoseTextDiffers)
{
    const auto schema
        = createSchema(std::vector<std::pair<std::string, DataType>>{{"u", DataType{Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE}}});
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1"}), "");
    ASSERT_EXCEPTION_ERRORCODE(getRowReport(schema, {"1"}, {"2"}), ErrorCode::UnknownDataType);
}

TEST_F(ResultComparisonTest, ListsAPairThatMatchesByValueWithBothTexts)
{
    EXPECT_EQ(getRowReport(createSchema({{"f", Type::FLOAT64}}), {"1.0", "2"}, {"1", "3"}), formatRowMismatch("1.0 | 1\n2 | _\n_ | 3"));
}

TEST_F(ResultComparisonTest, ChecksTheFieldCountOfAnExpectedRowAgainstTheExpectedSchema)
{
    const auto fieldA = formatField("A", "UINT64");
    const auto fieldB = formatField("B", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1"}, createSchema({{"a", Type::UINT64}}), {"2"}),
        formatSchemaAndRowMismatch(
            {fmt::format("{}, {} != {}", fieldA, fieldB, fieldA), formatMissingField(fieldB)},
            "1 | Not enough expected fields (expected sink schema has: 2, but got 1)\n_ | 2"));
}

TEST_F(ResultComparisonTest, ChecksTheFieldCountOnlyOfAPairedRowWhoseTextDiffers)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1"}, {}), formatRowMismatch("1 | _"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1"}), "");
}

TEST_F(ResultComparisonTest, SortsTheMissingValueOfAShortRowAsEmpty)
{
    const auto schema = createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(
        getRowReport(schema, {"1 2", "1"}, {"1 2"}),
        formatRowMismatch("1 | Not enough expected fields (expected sink schema has: 2, but got 1)\n1 2 | 1 2"));
    EXPECT_EQ(getRowReport(schema, {}, {"1 2", "1"}), formatRowMismatch("_ | 1\n___ | 1 2"));
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
    EXPECT_EQ(getRowReport(createSchema({{"a", Type::UINT64}}), {}, {""}), formatRowMismatch(" | "));
    EXPECT_EQ(
        getRowReport(createSchema({{"a", Type::UINT64}}), {""}, {"1"}),
        formatRowMismatch(" | Not enough expected fields (expected sink schema has: 1, but got 0)\n_ | 1"));
}

TEST_F(ResultComparisonTest, ComparesNamesCaseSensitivelyOnlyWhenTheExpectedNameIsQuoted)
{
    EXPECT_EQ(getReport(createSchema({{"Id", Type::UINT64}}), {}, createSchema({{"\"Id\"", Type::UINT64}}), {}), "");
    EXPECT_EQ(getReport(createSchema({{"\"ID\"", Type::UINT64}}), {}, createSchema({{"id", Type::UINT64}}), {}), "");

    const auto quoted = formatField("Id", "UINT64");
    const auto unquoted = formatField("ID", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"\"Id\"", Type::UINT64}}), {}, createSchema({{"Id", Type::UINT64}}), {}),
        formatSchemaMismatch({fmt::format("{} != {}", quoted, unquoted), formatMissingField(quoted), formatUnexpectedField(unquoted)}));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseTypeDiffers)
{
    const auto expectedField = formatField("ID", "UINT64");
    const auto actualField = formatField("ID", "INT64");
    EXPECT_EQ(
        getReport(createSchema({{"id", Type::UINT64}}), {"1"}, createSchema({{"id", Type::INT64}}), {"2"}),
        formatSchemaAndRowMismatch(
            {fmt::format("{} != {}", expectedField, actualField), formatMissingField(expectedField), formatUnexpectedField(actualField)},
            "1 _ | _ 2"));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseNullabilityDiffers)
{
    const auto expectedField = formatField("ID", "UINT64");
    const std::string actualField{"QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: true))"};
    EXPECT_EQ(
        getReport(
            createSchema({{"id", Type::UINT64}}),
            {"1"},
            createSchema(
                std::vector<std::pair<std::string, DataType>>{
                    {"id", DataTypeProvider::provideDataType(Type::UINT64, DataType::NULLABLE::IS_NULLABLE)}}),
            {"1"}),
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
    const auto fieldA = formatField("A", "UINT64");
    const auto fieldB = formatField("B", "UINT64");
    EXPECT_EQ(
        getReport(
            createSchema({{"a", Type::UINT64}, {"b", Type::UINT64}}),
            {"1 2"},
            createSchema({{"b", Type::UINT64}, {"a", Type::UINT64}}),
            {"2 1"}),
        formatSchemaAndRowMismatch(
            {fmt::format("{}, {} != {}, {}", fieldA, fieldB, fieldB, fieldA),
             formatMissingField(fieldA),
             formatMissingField(fieldB),
             formatUnexpectedField(fieldB),
             formatUnexpectedField(fieldA)},
            "1 2 _ _ | _ _ 2 1"));
}

TEST_F(ResultComparisonTest, KeepsTheInputOrderWhenNoFieldMatches)
{
    const auto fieldA = formatField("A", "UINT64");
    const auto fieldB = formatField("B", "UINT64");
    EXPECT_EQ(
        getReport(createSchema({{"a", Type::UINT64}}), {"2", "1"}, createSchema({{"b", Type::UINT64}}), {"1", "2"}),
        formatSchemaAndRowMismatch(
            {fmt::format("{} != {}", fieldA, fieldB), formatMissingField(fieldA), formatUnexpectedField(fieldB)}, "2 _ | _ 1\n1 _ | _ 2"));
}

}
