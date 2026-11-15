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

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
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

Schema<UnqualifiedUnboundField, Ordered> schemaOf(const std::vector<std::pair<std::string, DataType>>& fields)
{
    std::vector<UnqualifiedUnboundField> unboundFields;
    for (const auto& [name, dataType] : fields)
    {
        unboundFields.emplace_back(Identifier::parse(name), dataType);
    }
    return Schema<UnqualifiedUnboundField, Ordered>{std::move(unboundFields)};
}

Schema<UnqualifiedUnboundField, Ordered> schemaOf(const std::vector<std::pair<std::string, Type>>& fields)
{
    std::vector<std::pair<std::string, DataType>> typedFields;
    for (const auto& [name, type] : fields)
    {
        typedFields.emplace_back(name, DataTypeProvider::provideDataType(type));
    }
    return schemaOf(typedFields);
}

std::string getReport(
    const Schema<UnqualifiedUnboundField, Ordered>& expectedSchema,
    const std::vector<std::string>& expectedRows,
    const Schema<UnqualifiedUnboundField, Ordered>& actualSchema,
    const std::vector<std::string>& actualRows)
{
    const auto verdict = toVerdict(compare(expectedSchema, expectedRows, actualSchema, actualRows), ComparisonOrigin::SingleQuery);
    return verdict.has_value() ? std::string{} : verdict.error().detail;
}

std::string getRowReport(
    const Schema<UnqualifiedUnboundField, Ordered>& schema, const std::vector<std::string>& expectedRows, const std::vector<std::string>& actualRows)
{
    return getReport(schema, expectedRows, schema, actualRows);
}

std::string getValueReport(const Type type, const std::string& expected, const std::string& actual)
{
    return getRowReport(schemaOf({{"v", type}}), {expected}, {actual});
}

constexpr std::string_view SchemaHeader = "Schema Mismatch\n---------------\n";
constexpr std::string_view RowHeader = "Result Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"
                                       "-------------------------------------------------\n";

std::string rowMismatch(const std::string_view rows)
{
    return fmt::format("{}{}", RowHeader, rows);
}

std::string schemaMismatch(const std::string_view fields)
{
    return fmt::format("{}{}\n\nAll Results match", SchemaHeader, fields);
}

std::string schemaAndRowMismatch(const std::string_view fields, const std::string_view rows)
{
    return fmt::format("{}{}\n\n{}{}", SchemaHeader, fields, RowHeader, rows);
}
}

class ResultComparisonTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ResultComparisonTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(ResultComparisonTest, TwoEmptyResultsMatch)
{
    EXPECT_EQ(getRowReport(schemaOf({{"a", Type::UINT64}}), {}, {}), "");
}

TEST_F(ResultComparisonTest, ListsEveryRowWhenOneSideIsEmpty)
{
    const auto schema = schemaOf({{"a", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"3", "12"}, {}), rowMismatch("12 | __\n3 | _"));
    EXPECT_EQ(getRowReport(schema, {}, {"3", "12"}), rowMismatch("__ | 12\n_ | 3"));
}

TEST_F(ResultComparisonTest, IgnoresRowOrder)
{
    const auto schema = schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"3 1", "1 2", "2 3"}, {"2 3", "3 1", "1 2"}), "");
}

TEST_F(ResultComparisonTest, ComparesRowsAsAMultiset)
{
    const auto schema = schemaOf({{"a", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1", "1"}, {"1"}), rowMismatch("1 | 1\n1 | _"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1", "1"}), rowMismatch("1 | 1\n_ | 1"));
}

TEST_F(ResultComparisonTest, SortsRowsAsTextInTheReport)
{
    EXPECT_EQ(getRowReport(schemaOf({{"a", Type::UINT64}}), {"9", "10"}, {"9", "11"}), rowMismatch("10 | __\n__ | 11\n9 | 9"));
}

TEST_F(ResultComparisonTest, SkipsTheSmallerRowOfAMismatchingPair)
{
    const auto schema = schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1 2", "1 1"}, {"1 1", "1 3"}), rowMismatch("1 1 | 1 1\n1 2 | ___\n___ | 1 3"));
    EXPECT_EQ(getRowReport(schema, {"2 2"}, {"1 1", "2 2"}), rowMismatch("___ | 1 1\n2 2 | 2 2"));
}

TEST_F(ResultComparisonTest, TreatsCommasAndRunsOfSpacesAsOneSeparator)
{
    const auto schema = schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1,2,3"}, {"1 2 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1, 2,3"}, {"1 2 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1  2   3"}, {"1,2,3"}), "");
}

TEST_F(ResultComparisonTest, CountsALeadingOrTrailingSpaceAsAnEmptyField)
{
    const auto schema = schemaOf({{"a", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {" 1"}, {"1"}), rowMismatch(" 1 | Too many expected fields (expected sink schema has: 1, but got 2)\n_ | 1"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1 "}), rowMismatch("1 | 1"));
    EXPECT_EQ(getRowReport(schema, {"1"}, {" 1"}), rowMismatch("__ |  1\n1 | _"));
}

TEST_F(ResultComparisonTest, ReadsAnEmptyCommaSeparatedColumnAsNull)
{
    const auto schema = schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {"1,,3"}, {"1 NULL 3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1,2,"}, {"1 2 null"}), "");
    EXPECT_EQ(getRowReport(schema, {",2,3"}, {"NULL,2,3"}), "");
    EXPECT_EQ(getRowReport(schema, {"1 NULL 3"}, {"1,,3"}), "");
    EXPECT_EQ(getRowReport(schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}}), {","}, {"NULL NULL"}), "");
}

TEST_F(ResultComparisonTest, DoesNotReadAnEmptySpaceSeparatedColumnAsNull)
{
    EXPECT_EQ(
        getReport(
            schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}, {"c", Type::UINT64}}),
            {"1  3"},
            schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}}),
            {"1 NULL"}),
        schemaAndRowMismatch(
            "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: B, type: "
            "DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: C, type: DataType(type: UINT64 nullable: false)) != "
            "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: B, type: "
            "DataType(type: UINT64 nullable: false))\n"
            "- 'QualifiedUnboundField: (name: C, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.",
            "1 3 | Not enough expected fields (expected sink schema has: 3, but got 2)\n______ | 1 NULL"));
}

TEST_F(ResultComparisonTest, MatchesNullOnlyWithNullInAnyCase)
{
    EXPECT_EQ(getValueReport(Type::UINT64, "NULL", "null"), "");
    EXPECT_EQ(getValueReport(Type::VARSIZED, "Null", "NULL"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "null", "NULL"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "NULL", "nUlL"), "");
    EXPECT_EQ(getValueReport(Type::UINT64, "NULL", "0"), rowMismatch("_ | 0\nNULL | ____"));
    EXPECT_EQ(getValueReport(Type::UINT64, "0", "NULL"), rowMismatch("0 | _\n____ | NULL"));
}

TEST_F(ResultComparisonTest, SortsNullByItsTextBeforeMatchingItInAnyCase)
{
    const auto schema = schemaOf({{"s", Type::VARSIZED}, {"n", Type::UINT64}});
    EXPECT_EQ(getRowReport(schema, {",1", "a,2"}, {"null,1", "a,2"}), rowMismatch("NULL 1 | ______\na 2 | a 2\n______ | null 1"));
}

TEST_F(ResultComparisonTest, ComparesIntegersAsText)
{
    EXPECT_EQ(getValueReport(Type::UINT64, "01", "1"), rowMismatch("01 | __\n_ | 1"));
    EXPECT_EQ(getValueReport(Type::INT32, "-1", "-1"), "");
    EXPECT_EQ(getValueReport(Type::INT8, "+1", "1"), rowMismatch("+1 | __\n_ | 1"));
}

TEST_F(ResultComparisonTest, ComparesFloatsWithinARelativeEpsilonOfTheSum)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.000001"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.00002"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.000021"), rowMismatch("1.0 | ___\n________ | 1.000021"));
    EXPECT_EQ(getValueReport(Type::FLOAT64, "-1.0", "-1.00002"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1000000", "1000020"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT32, "1.0", "1.00002"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT32, "1.0", "1.000021"), rowMismatch("1.0 | ___\n________ | 1.000021"));
}

TEST_F(ResultComparisonTest, ComparesFloatsByValueWhenTheTextDiffers)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0", "1.00000001"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1", "1.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1e1", "10.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "inf", "INF"), "");
}

TEST_F(ResultComparisonTest, ComparesAFloatWithZeroAlmostExactly)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0", "0.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "-0.0", "0.0"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0", "0.0000001"), rowMismatch("0 | _\n_________ | 0.0000001"));
    EXPECT_EQ(getValueReport(Type::FLOAT64, "0.000001", "-0.000001"), rowMismatch("_________ | -0.000001\n0.000001 | ________"));
}

TEST_F(ResultComparisonTest, SortsSignedZerosApartBeforeMatchingThem)
{
    EXPECT_EQ(
        getRowReport(schemaOf({{"f", Type::FLOAT64}}), {"-0.0", "-1.0"}, {"0.0", "-1.0"}), rowMismatch("-0.0 | ____\n-1.0 | -1.0\n___ | 0.0"));
}

TEST_F(ResultComparisonTest, ParsesOnlyTheNumericPrefixOfAFloat)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "1.0abc", "1.0"), "");
}

TEST_F(ResultComparisonTest, NeverMatchesTwoNansThatDifferAsText)
{
    EXPECT_EQ(getValueReport(Type::FLOAT64, "nan", "nan"), "");
    EXPECT_EQ(getValueReport(Type::FLOAT64, "nan", "NaN"), rowMismatch("___ | NaN\nnan | ___"));
}

TEST_F(ResultComparisonTest, ThrowsOnAFloatThatIsNotANumber)
{
    const auto schema = schemaOf({{"f", Type::FLOAT64}});
    EXPECT_THROW((void)getRowReport(schema, {"abc"}, {"1.0"}), std::invalid_argument);
    EXPECT_THROW((void)getRowReport(schema, {"1.0"}, {"abc"}), std::invalid_argument);
    EXPECT_EQ(getRowReport(schema, {"abc"}, {"abc"}), "");
}

TEST_F(ResultComparisonTest, ReadsBooleansAsWordsOrIntegers)
{
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "true", "1"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "TRUE", "true"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "False", "0"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "2", "1"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "-1", "true"), "");
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "true", "0"), rowMismatch("_ | 0\ntrue | ____"));
}

TEST_F(ResultComparisonTest, AcceptsAnyBooleanTextThatMatchesExactly)
{
    EXPECT_EQ(getValueReport(Type::BOOLEAN, "yes", "yes"), "");
}

TEST_F(ResultComparisonTest, StopsOnABooleanThatIsNeitherWordNorInteger)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const auto schema = schemaOf({{"b", Type::BOOLEAN}});
    EXPECT_DEATH_DEBUG((void)getRowReport(schema, {"yes"}, {"true"}), "");
}

TEST_F(ResultComparisonTest, ComparesTextExactly)
{
    EXPECT_EQ(getValueReport(Type::VARSIZED, "abc", "ABC"), rowMismatch("___ | ABC\nabc | ___"));
    EXPECT_EQ(getValueReport(Type::CHAR, "a", "A"), rowMismatch("_ | A\na | _"));
    EXPECT_EQ(getValueReport(Type::CHAR, "a", "a"), "");
}

TEST_F(ResultComparisonTest, SplitsTextOnSpacesAndCommas)
{
    const auto schema = schemaOf({{"s", Type::VARSIZED}});
    EXPECT_EQ(getRowReport(schema, {"a b"}, {"a b"}), "");
    EXPECT_EQ(getRowReport(schema, {"a,b"}, {"a b"}), "");
    EXPECT_EQ(getRowReport(schema, {"a  b"}, {"a b"}), "");
    EXPECT_EQ(
        getRowReport(schema, {"a b"}, {"a c"}), rowMismatch("a b | Too many expected fields (expected sink schema has: 1, but got 2)\n___ | a c"));
}

TEST_F(ResultComparisonTest, ThrowsOnAnUndefinedFieldWhoseTextDiffers)
{
    const auto schema
        = schemaOf(std::vector<std::pair<std::string, DataType>>{{"u", DataType{Type::UNDEFINED, DataType::NULLABLE::NOT_NULLABLE}}});
    EXPECT_EQ(getRowReport(schema, {"1"}, {"1"}), "");
    ASSERT_EXCEPTION_ERRORCODE((void)getRowReport(schema, {"1"}, {"2"}), ErrorCode::UnknownDataType);
}

TEST_F(ResultComparisonTest, ChecksTheFieldCountOfAnExpectedRowAgainstTheExpectedSchema)
{
    EXPECT_EQ(
        getReport(schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1"}, schemaOf({{"a", Type::UINT64}}), {"2"}),
        schemaAndRowMismatch(
            "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: B, type: "
            "DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false))\n"
            "- 'QualifiedUnboundField: (name: B, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.",
            "1 | Not enough expected fields (expected sink schema has: 2, but got 1)\n_ | 2"));
    EXPECT_EQ(
        getRowReport(schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1 2 3"}, {"1 2"}),
        rowMismatch("1 2 3 | Too many expected fields (expected sink schema has: 2, but got 3)\n___ | 1 2"));
}

TEST_F(ResultComparisonTest, ReportsAnActualRowWithAnExtraFieldBesideItsMatchingFields)
{
    EXPECT_EQ(getRowReport(schemaOf({{"a", Type::UINT64}}), {"1"}, {"1 2"}), rowMismatch("1 | 1"));
}

TEST_F(ResultComparisonTest, MatchesUnquotedNamesInAnyCase)
{
    EXPECT_EQ(getReport(schemaOf({{"id", Type::UINT64}}), {"1"}, schemaOf({{"ID", Type::UINT64}}), {"1"}), "");
}

TEST_F(ResultComparisonTest, MatchesQuotedNamesOnlyWhenTheExpectedNameIsQuoted)
{
    EXPECT_EQ(getReport(schemaOf({{"\"Id\"", Type::UINT64}}), {}, schemaOf({{"\"Id\"", Type::UINT64}}), {}), "");
    EXPECT_EQ(getReport(schemaOf({{"Id", Type::UINT64}}), {}, schemaOf({{"\"Id\"", Type::UINT64}}), {}), "");
    EXPECT_EQ(getReport(schemaOf({{"\"ID\"", Type::UINT64}}), {}, schemaOf({{"id", Type::UINT64}}), {}), "");
    EXPECT_EQ(
        getReport(schemaOf({{"\"Id\"", Type::UINT64}}), {}, schemaOf({{"Id", Type::UINT64}}), {}),
        schemaMismatch(
            "QualifiedUnboundField: (name: Id, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: "
            "DataType(type: UINT64 nullable: false))\n"
            "- 'QualifiedUnboundField: (name: Id, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
            "+ 'QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result "
            "schema."));
    EXPECT_EQ(
        getReport(schemaOf({{"\"Id\"", Type::UINT64}}), {}, schemaOf({{"\"id\"", Type::UINT64}}), {}),
        schemaMismatch(
            "QualifiedUnboundField: (name: Id, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: id, type: "
            "DataType(type: UINT64 nullable: false))\n"
            "- 'QualifiedUnboundField: (name: Id, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
            "+ 'QualifiedUnboundField: (name: id, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result "
            "schema."));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseTypeDiffers)
{
    const auto expectedSchema = schemaOf({{"id", Type::UINT64}});
    const auto actualSchema = schemaOf({{"id", Type::INT64}});
    constexpr std::string_view fields
        = "QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: "
          "DataType(type: INT64 nullable: false))\n"
          "- 'QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
          "+ 'QualifiedUnboundField: (name: ID, type: DataType(type: INT64 nullable: false))' is unexpected field in actual result schema.";
    EXPECT_EQ(getReport(expectedSchema, {"1"}, actualSchema, {"2"}), schemaAndRowMismatch(fields, "1 _ | _ 2"));
    EXPECT_EQ(getReport(expectedSchema, {"1"}, actualSchema, {"1"}), schemaMismatch(fields));
}

TEST_F(ResultComparisonTest, ReportsAFieldWhoseNullabilityDiffers)
{
    EXPECT_EQ(
        getReport(
            schemaOf({{"id", Type::UINT64}}),
            {"1"},
            schemaOf(
                std::vector<std::pair<std::string, DataType>>{
                    {"id", DataTypeProvider::provideDataType(Type::UINT64, DataType::NULLABLE::IS_NULLABLE)}}),
            {"1"}),
        schemaMismatch(
            "QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: "
            "DataType(type: UINT64 nullable: true))\n"
            "- 'QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
            "+ 'QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: true))' is unexpected field in actual result "
            "schema."));
}

TEST_F(ResultComparisonTest, FillsTheColumnsOfAnUnexpectedFieldWithPlaceholders)
{
    const auto expectedSchema = schemaOf({{"id", Type::UINT64}});
    const auto actualSchema = schemaOf({{"id", Type::UINT64}, {"extra", Type::UINT64}});
    constexpr std::string_view fields
        = "QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: "
          "DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: EXTRA, type: DataType(type: UINT64 nullable: false))\n"
          "+ 'QualifiedUnboundField: (name: EXTRA, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result "
          "schema.";
    EXPECT_EQ(getReport(expectedSchema, {"1"}, actualSchema, {"1,2"}), schemaAndRowMismatch(fields, "1 _ | 1 2"));
    EXPECT_EQ(getReport(expectedSchema, {"1"}, actualSchema, {"2,2"}), schemaAndRowMismatch(fields, "1 | _\n___ | 2 2"));
}

TEST_F(ResultComparisonTest, FillsTheColumnsOfAMissingFieldWithPlaceholders)
{
    const auto expectedSchema = schemaOf({{"id", Type::UINT64}, {"value", Type::UINT64}});
    const auto actualSchema = schemaOf({{"id", Type::UINT64}});
    constexpr std::string_view fields
        = "QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: VALUE, type: "
          "DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false))\n"
          "- 'QualifiedUnboundField: (name: VALUE, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.";
    EXPECT_EQ(getReport(expectedSchema, {"1,2"}, actualSchema, {"1"}), schemaAndRowMismatch(fields, "1 2 | 1 _"));
    EXPECT_EQ(getReport(expectedSchema, {"1,2"}, actualSchema, {"3"}), schemaAndRowMismatch(fields, "1 2 | ___\n_ | 3"));
    EXPECT_EQ(getReport(expectedSchema, {"1"}, actualSchema, {"1"}), schemaMismatch(fields));
}

TEST_F(ResultComparisonTest, ReportsSwappedFieldsAsMissingAndUnexpected)
{
    EXPECT_EQ(
        getReport(
            schemaOf({{"a", Type::UINT64}, {"b", Type::UINT64}}), {"1 2"}, schemaOf({{"b", Type::UINT64}, {"a", Type::UINT64}}), {"2 1"}),
        schemaAndRowMismatch(
            "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: B, type: "
            "DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: B, type: DataType(type: UINT64 nullable: false)), "
            "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false))\n"
            "- 'QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
            "- 'QualifiedUnboundField: (name: B, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
            "+ 'QualifiedUnboundField: (name: B, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result "
            "schema.\n"
            "+ 'QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result "
            "schema.",
            "1 2 _ _ | _ _ 2 1"));
}

TEST_F(ResultComparisonTest, KeepsTheInputOrderWhenNoFieldMatches)
{
    const auto expectedSchema = schemaOf({{"a", Type::UINT64}});
    const auto actualSchema = schemaOf({{"b", Type::UINT64}});
    constexpr std::string_view fields
        = "QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: B, type: "
          "DataType(type: UINT64 nullable: false))\n"
          "- 'QualifiedUnboundField: (name: A, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.\n"
          "+ 'QualifiedUnboundField: (name: B, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result schema.";
    EXPECT_EQ(getReport(expectedSchema, {"1", "2"}, actualSchema, {"1", "2"}), schemaMismatch(fields));
    EXPECT_EQ(getReport(expectedSchema, {"2", "1"}, actualSchema, {"1", "2"}), schemaAndRowMismatch(fields, "2 _ | _ 1\n1 _ | _ 2"));
}

}
