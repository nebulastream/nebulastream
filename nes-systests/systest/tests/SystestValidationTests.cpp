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

#include <chrono>
#include <cstddef>
#include <expected>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <gtest/gtest.h>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SystestPreparation.hpp>
#include <SystestQueryModel.hpp>
#include <SystestValidation.hpp>

namespace NES::Systest
{
namespace
{

ResultSchema schema(std::initializer_list<std::pair<std::string_view, DataType::Type>> fields)
{
    std::vector<UnqualifiedUnboundField> result;
    result.reserve(fields.size());
    for (const auto& [name, type] : fields)
    {
        result.emplace_back(Identifier::parse(std::string{name}), DataTypeProvider::provideDataType(type));
    }
    return ResultSchema{std::move(result)};
}

TestCaseId testId()
{
    return TestCaseId{
        .source = CaseKey{.relativeTestFile = "validation.test", .queryNumber = SystestQueryId{1}}, .configurationVariant = 0};
}

Origin testOrigin()
{
    return Origin{.file = "validation.test", .firstLine = 4, .lastLine = 8};
}

ResolvedCase testCase(
    CaseExpectation expectation, CaseAction action = QueryAction{.sql = "SELECT value FROM input INTO File();", .kind = QueryKind::Execute})
{
    return ResolvedCase{
        .id = testId(),
        .environment = EnvironmentId{.value = 1},
        .source = testOrigin(),
        .action = std::move(action),
        .expectation = std::move(expectation),
        .dependencies = {}};
}

DistributedLogicalPlan emptyDistributedPlan()
{
    auto globalPlan = LogicalPlan{QueryId::createDistributed(DistributedQueryId{"validation-test"}), std::vector<LogicalOperator>{}};
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
    localPlans.emplace(Host{"localhost"}, std::vector<LogicalPlan>{globalPlan});
    return DistributedLogicalPlan{std::move(localPlans), std::move(globalPlan)};
}

PreparedExecutionCatalog preparedQueryCatalog(ResultSchema outputSchema, OutputTarget output)
{
    auto action = std::make_shared<const PreparedAction>(PreparedQuery{
        .statement = PreparedStatement{
            .sql = "SELECT value FROM input INTO File();",
            .plan = emptyDistributedPlan(),
            .outputSchema = std::move(outputSchema),
            .output = std::move(output),
            .sourceMetrics = {}}});
    std::map<TestCaseId, PreparedExecution> executions;
    executions.emplace(testId(), PreparedExecution{.environment = EnvironmentId{.value = 1}, .prepared = std::move(action)});
    return PreparedExecutionCatalog{std::move(executions)};
}

class RecordingResultDecoder final : public ResultDecoder
{
public:
    explicit RecordingResultDecoder(DecodedTable table) : table(std::move(table)) { }

    std::expected<DecodedTable, ValidationDiagnostic> decode(const TableArtifact&) const override
    {
        ++calls;
        return table;
    }

    mutable size_t calls = 0;

private:
    DecodedTable table;
};

class ThrowingResultDecoder final : public ResultDecoder
{
public:
    std::expected<DecodedTable, ValidationDiagnostic> decode(const TableArtifact&) const override { throw TestException("decoder failed"); }
};

ValidatedResult validateExpectedError(const ErrorExpectation& expectation, ExecutionError error)
{
    PreparedExecutionCatalog catalog{std::map<TestCaseId, PreparedExecution>{}};
    RecordingResultDecoder decoder{DecodedTable{}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(expectation);
    return validator.validate(
        definition,
        FailedExecution{.id = testId(), .stage = ExecutionStage::Planning, .error = std::move(error), .artifacts = {}});
}

TEST(SystestValidationTest, FileResultDecoderLoadsSchemaAndRows)
{
    const auto file = std::filesystem::temp_directory_path()
        / ("systest-validation-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".csv");
    {
        std::ofstream output(file);
        output << "value:UINT64:NOT_NULLABLE\n2\n1\n";
    }

    const auto decoded = FileResultDecoder{}.decode(TableArtifact{.file = file});

    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->schema, schema({{"value", DataType::Type::UINT64}}));
    EXPECT_EQ(decoded->rows, (std::vector<std::string>{"2", "1"}));
    std::filesystem::remove(file);
}

TEST(SystestValidationTest, FileResultDecoderReportsUnreadableFile)
{
    const auto file = std::filesystem::temp_directory_path() / "missing-systest-validation-result.csv";
    std::filesystem::remove(file);

    const auto decoded = FileResultDecoder{}.decode(TableArtifact{.file = file});

    ASSERT_FALSE(decoded);
    EXPECT_EQ(decoded.error().kind, DiagnosticKind::Validation);
    EXPECT_TRUE(decoded.error().message.contains("Failed to load query result from"));
    EXPECT_TRUE(decoded.error().message.contains(file.string()));
}

TEST(SystestValidationTest, ResultComparatorMatchesUnorderedTypedRows)
{
    const auto outputSchema
        = schema({{"id", DataType::Type::UINT64}, {"enabled", DataType::Type::BOOLEAN}, {"score", DataType::Type::FLOAT64}});
    const RowsExpectation expected{.rows = {"2,false,4.0", "1,true,3.0"}, .comparison = ComparisonPolicy::UnorderedTypedRows};
    const DecodedInputStream actual{.schema = outputSchema, .rows = {"1,1,3.000001", "2,0,4.000001"}};

    const auto comparison = ResultComparator{}.compare(expected, outputSchema, actual);

    EXPECT_TRUE(comparison.matches);
    EXPECT_TRUE(comparison.diagnostics.empty());
}

TEST(SystestValidationTest, ResultComparatorRejectsIdenticalRowsShorterThanTheSchema)
{
    const auto outputSchema = schema({{"id", DataType::Type::UINT64}, {"name", DataType::Type::VARSIZED}});
    const RowsExpectation expected{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows};
    const DecodedInputStream actual{.schema = outputSchema, .rows = {"1"}};

    const auto comparison = ResultComparator{}.compare(expected, outputSchema, actual);

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("expected sink schema has: 2, but got 1"));
}

TEST(SystestValidationTest, ResultComparatorRejectsFloatingPointValuesWithTrailingCharacters)
{
    const auto outputSchema = schema({{"score", DataType::Type::FLOAT64}});
    const RowsExpectation expected{.rows = {"1oops"}, .comparison = ComparisonPolicy::UnorderedTypedRows};
    const DecodedInputStream actual{.schema = outputSchema, .rows = {"1.0"}};

    const auto comparison = ResultComparator{}.compare(expected, outputSchema, actual);

    EXPECT_FALSE(comparison.matches);
}

TEST(SystestValidationTest, ResultComparatorRejectsUnsupportedComparisonPolicy)
{
    const auto outputSchema = schema({{"id", DataType::Type::UINT64}});
    const RowsExpectation expected{.rows = {"1"}, .comparison = static_cast<ComparisonPolicy>(255)};
    const DecodedInputStream actual{.schema = outputSchema, .rows = {"1"}};

    const auto comparison = ResultComparator{}.compare(expected, outputSchema, actual);

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("Unsupported row comparison policy"));
}

TEST(SystestValidationTest, ResultComparatorPreservesMismatchDiagnostics)
{
    const auto outputSchema = schema({{"id", DataType::Type::UINT64}});
    const RowsExpectation expected{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows};
    const DecodedInputStream actual{.schema = outputSchema, .rows = {"2"}};

    const auto comparison = ResultComparator{}.compare(expected, outputSchema, actual);

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_EQ(comparison.diagnostics.front().kind, DiagnosticKind::Validation);
    EXPECT_TRUE(
        comparison.diagnostics.front().message.starts_with("\n\nResult Mismatch\nExpected Results(Sorted) | Actual Results(Sorted)\n"));
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("1 | _"));
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("_ | 2"));
}

TEST(SystestValidationTest, ResultComparatorPreservesSchemaMismatchDiagnostics)
{
    const auto expectedSchema = schema({{"id", DataType::Type::UINT64}});
    const auto actualSchema = schema({{"other", DataType::Type::UINT64}});
    const RowsExpectation expected{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows};
    const DecodedInputStream actual{.schema = actualSchema, .rows = {"1"}};

    const auto comparison = ResultComparator{}.compare(expected, expectedSchema, actual);

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.starts_with("\n\nSchema Mismatch\n---------------"));
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("All Results match"));
}

TEST(SystestValidationTest, DifferentialResultMismatchIsAnnotated)
{
    const auto outputSchema = schema({{"id", DataType::Type::UINT64}});
    const DecodedInputStream left{.schema = outputSchema, .rows = {"1"}};
    const DecodedInputStream right{.schema = outputSchema, .rows = {"2"}};

    const auto comparison = ResultComparator{}.compare(left, right);

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.ends_with("\n\nThis error happend during differential query execution."));
}

TEST(SystestValidationTest, TextComparatorKeepsExactLineMatching)
{
    const TextExpectation expected{.lines = {"== Optimized Plan ==", "SINK(FILE)"}, .matching = TextMatchPolicy::Automatic};

    const auto matching = TextComparator{}.compare(expected, "== Optimized Plan ==\nSINK(FILE)\n");
    const auto mismatching = TextComparator{}.compare(expected, "== Optimized Plan ==\nSINK(OTHER)\n");

    EXPECT_TRUE(matching.matches);
    ASSERT_FALSE(mismatching.matches);
    ASSERT_EQ(mismatching.diagnostics.size(), 1);
    EXPECT_TRUE(mismatching.diagnostics.front().message.contains("first difference at line 2"));
}

TEST(SystestValidationTest, TextComparatorMatchesRegexAssertions)
{
    const TextExpectation expected{
        .lines
        = {"<REGEX>== Optimized Plan ==</REGEX>",
           "<REGEX>",
           R"(SINK\(SINK[0-9]+\))",
           R"(  SOURCE\(stream_[0-9]+\))",
           "</REGEX>",
           "<!REGEX>SELECTION</!REGEX>"},
        .matching = TextMatchPolicy::Automatic};

    const auto comparison = TextComparator{}.compare(expected, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n");

    EXPECT_TRUE(comparison.matches);
    EXPECT_TRUE(comparison.diagnostics.empty());
}

TEST(SystestValidationTest, TextComparatorReportsMissingPositiveRegexMatch)
{
    const TextExpectation expected{.lines = {R"(<REGEX>SELECTION\(VALUE > 2\)</REGEX>)"}, .matching = TextMatchPolicy::Automatic};

    const auto comparison = TextComparator{}.compare(expected, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n");

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("expected pattern \"SELECTION\\(VALUE > 2\\)\" to match"));
}

TEST(SystestValidationTest, TextComparatorReportsUnexpectedNegativeRegexMatch)
{
    const TextExpectation expected{.lines = {"<!REGEX>SELECTION</!REGEX>"}, .matching = TextMatchPolicy::Automatic};

    const auto comparison
        = TextComparator{}.compare(expected, "== Optimized Plan ==\nSINK(SINK42)\n  SELECTION(VALUE > 2)\n    SOURCE(stream_17)\n");

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("expected pattern \"SELECTION\" not to match"));
}

TEST(SystestValidationTest, TextComparatorRejectsMixedRegexMatchingModes)
{
    const TextExpectation expected{.lines = {"<REGEX>SINK</REGEX>", "SOURCE(stream)"}, .matching = TextMatchPolicy::Automatic};

    const auto comparison = TextComparator{}.compare(expected, "SINK\nSOURCE(stream)");

    ASSERT_FALSE(comparison.matches);
    ASSERT_EQ(comparison.diagnostics.size(), 1);
    EXPECT_TRUE(comparison.diagnostics.front().message.contains("tagged and untagged expected output must not be mixed"));
}

TEST(SystestValidationTest, TextComparatorRejectsMalformedRegexTags)
{
    const std::vector<std::vector<std::string>> invalidExpectedResults{
        {"<REGEX>", "SINK"},
        {"<REGEX>", "SINK", "</!REGEX>"},
        {"<REGEX><!REGEX>SINK</!REGEX></REGEX>"},
        {"<REGEX></REGEX>"},
        {"<!REGEX>", "</!REGEX>"}};

    for (const auto& lines : invalidExpectedResults)
    {
        const auto comparison = TextComparator{}.compare(TextExpectation{.lines = lines, .matching = TextMatchPolicy::Automatic}, "SINK");
        ASSERT_FALSE(comparison.matches);
        ASSERT_EQ(comparison.diagnostics.size(), 1);
        EXPECT_TRUE(comparison.diagnostics.front().message.contains("Invalid Explain Regex Assertion"));
    }
}

TEST(SystestValidationTest, CaseValidatorUsesPreparedQuerySchema)
{
    const auto outputSchema = schema({{"value", DataType::Type::UINT64}});
    auto catalog = preparedQueryCatalog(outputSchema, OutputTarget{.kind = OutputTargetKind::Table, .file = "validation-result.csv"});
    RecordingResultDecoder decoder{DecodedTable{.schema = outputSchema, .rows = {"1"}}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(RowsExpectation{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows});
    const CompletedExecution completed{
        .id = testId(),
        .outputs = {TableArtifact{.file = "validation-result.csv"}},
        .metrics = ExecutionMetrics{.started = {}, .finished = {}, .bytesProcessed = 17, .tuplesProcessed = 3},
        .artifacts = ArtifactSet{.files = {"validation-result.csv"}}};

    const auto result = validator.validate(definition, ExecutionOutcome{completed});

    EXPECT_EQ(result.verdict, Verdict::Passed);
    EXPECT_TRUE(result.diagnostics.empty());
    EXPECT_EQ(result.metrics.bytesProcessed, 17);
    EXPECT_EQ(result.metrics.tuplesProcessed, 3);
    EXPECT_EQ(result.artifacts.files, (std::vector<std::filesystem::path>{"validation-result.csv"}));
    EXPECT_EQ(decoder.calls, 1);
}

TEST(SystestValidationTest, CaseValidatorRetainsMetricsAndArtifactsWhenValidationThrows)
{
    const auto outputSchema = schema({{"value", DataType::Type::UINT64}});
    auto catalog = preparedQueryCatalog(outputSchema, OutputTarget{.kind = OutputTargetKind::Table, .file = "validation-result.csv"});
    const ThrowingResultDecoder decoder;
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(RowsExpectation{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows});
    const CompletedExecution completed{
        .id = testId(),
        .outputs = {TableArtifact{.file = "validation-result.csv"}},
        .metrics = ExecutionMetrics{.started = {}, .finished = {}, .bytesProcessed = 17, .tuplesProcessed = 3},
        .artifacts = ArtifactSet{.files = {"validation-result.csv"}}};

    const auto result = validator.validate(definition, ExecutionOutcome{completed});

    EXPECT_EQ(result.verdict, Verdict::Failed);
    ASSERT_EQ(result.diagnostics.size(), 1);
    EXPECT_TRUE(result.diagnostics.front().message.contains("decoder failed"));
    EXPECT_EQ(result.metrics.bytesProcessed, 17);
    EXPECT_EQ(result.metrics.tuplesProcessed, 3);
    EXPECT_EQ(result.artifacts.files, (std::vector<std::filesystem::path>{"validation-result.csv"}));
}

TEST(SystestValidationTest, CaseValidatorRejectsRowsShorterThanThePreparedSchema)
{
    const auto outputSchema = schema({{"id", DataType::Type::UINT64}, {"name", DataType::Type::VARSIZED}});
    auto catalog = preparedQueryCatalog(outputSchema, OutputTarget{.kind = OutputTargetKind::Table, .file = "validation-result.csv"});
    RecordingResultDecoder decoder{DecodedTable{.schema = outputSchema, .rows = {"1", "1"}}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(RowsExpectation{.rows = {"1,first", "1,second"}, .comparison = ComparisonPolicy::UnorderedTypedRows});
    const CompletedExecution completed{
        .id = testId(),
        .outputs = {TableArtifact{.file = "validation-result.csv"}},
        .metrics = {},
        .artifacts = ArtifactSet{.files = {"validation-result.csv"}}};

    const auto result = validator.validate(definition, ExecutionOutcome{completed});

    EXPECT_EQ(result.verdict, Verdict::Failed);
    EXPECT_FALSE(result.diagnostics.empty());
    EXPECT_EQ(result.artifacts.files, (std::vector<std::filesystem::path>{"validation-result.csv"}));
}

TEST(SystestValidationTest, CaseValidatorUsesPreparedDiscardTarget)
{
    auto catalog
        = preparedQueryCatalog(schema({{"value", DataType::Type::UINT64}}), OutputTarget{.kind = OutputTargetKind::Discard, .file = {}});
    RecordingResultDecoder decoder{DecodedTable{}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(RowsExpectation{.rows = {"unused"}, .comparison = ComparisonPolicy::UnorderedTypedRows});
    const CompletedExecution completed{.id = testId(), .outputs = {DiscardedOutput{}}, .metrics = {}, .artifacts = {}};

    const auto result = validator.validate(definition, ExecutionOutcome{completed});

    EXPECT_EQ(result.verdict, Verdict::Passed);
    EXPECT_TRUE(result.diagnostics.empty());
    EXPECT_EQ(decoder.calls, 0);
}

TEST(SystestValidationTest, ExpectedErrorMessageMatchesSubstringForTheExpectedCode)
{
    const auto result = validateExpectedError(
        ErrorExpectation{.code = ErrorCode::TestException, .message = "specific failure"},
        ExecutionError{
            .kind = ExecutionErrorKind::Statement,
            .details = {{.code = ErrorCode::TestException, .message = "worker-1: specific failure with context"}}});

    EXPECT_EQ(result.verdict, Verdict::Passed);
    EXPECT_TRUE(result.diagnostics.empty());
}

TEST(SystestValidationTest, ExpectedErrorMessageRejectsWrongMessageForMatchingCode)
{
    const auto result = validateExpectedError(
        ErrorExpectation{.code = ErrorCode::TestException, .message = "expected detail"},
        ExecutionError{
            .kind = ExecutionErrorKind::Statement,
            .details = {{.code = ErrorCode::TestException, .message = "different detail"}}});

    ASSERT_EQ(result.verdict, Verdict::Failed);
    ASSERT_EQ(result.diagnostics.size(), 1);
    EXPECT_TRUE(result.diagnostics.front().message.contains("expected detail"));
    EXPECT_TRUE(result.diagnostics.front().message.contains("different detail"));
}

TEST(SystestValidationTest, ExpectedErrorMessageMatchesOneOfMultipleDetailsWithTheExpectedCode)
{
    const auto result = validateExpectedError(
        ErrorExpectation{.code = ErrorCode::TestException, .message = "matching detail"},
        ExecutionError{
            .kind = ExecutionErrorKind::Statement,
            .details
            = {{.code = ErrorCode::InvalidConfigParameter, .message = "unrelated"},
               {.code = ErrorCode::TestException, .message = "host: matching detail with context"}}});

    EXPECT_EQ(result.verdict, Verdict::Passed);
}

TEST(SystestValidationTest, ExpectedErrorMessageDoesNotMatchDetailWithAnotherCode)
{
    const auto result = validateExpectedError(
        ErrorExpectation{.code = ErrorCode::TestException, .message = "matching text"},
        ExecutionError{
            .kind = ExecutionErrorKind::Statement,
            .details
            = {{.code = ErrorCode::TestException, .message = "wrong text"},
               {.code = ErrorCode::InvalidConfigParameter, .message = "matching text"}}});

    ASSERT_EQ(result.verdict, Verdict::Failed);
    ASSERT_EQ(result.diagnostics.size(), 1);
    EXPECT_TRUE(result.diagnostics.front().message.contains("matching text"));
    EXPECT_TRUE(result.diagnostics.front().message.contains("wrong text"));
}

TEST(SystestValidationTest, BackendFailureDoesNotSatisfyExpectedStatementError)
{
    PreparedExecutionCatalog catalog{std::map<TestCaseId, PreparedExecution>{}};
    RecordingResultDecoder decoder{DecodedTable{}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(ErrorExpectation{.code = ErrorCode::TestException, .message = "backend infrastructure failure"});
    const FailedExecution failed{
        .id = testId(),
        .stage = ExecutionStage::Starting,
        .error
        = ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = ErrorCode::TestException, .message = "backend infrastructure failure"}}},
        .artifacts = {}};

    const auto result = validator.validate(definition, ExecutionOutcome{failed});

    ASSERT_EQ(result.verdict, Verdict::Failed);
    ASSERT_EQ(result.diagnostics.size(), 1);
    EXPECT_EQ(result.diagnostics.front().kind, DiagnosticKind::Execution);
    EXPECT_EQ(result.diagnostics.front().message, "Query Failed with unexpected error: backend infrastructure failure");
    EXPECT_EQ(result.diagnostics.front().source, testOrigin());
}

TEST(SystestValidationTest, StatementFailureSatisfiesExpectedStatementError)
{
    PreparedExecutionCatalog catalog{std::map<TestCaseId, PreparedExecution>{}};
    RecordingResultDecoder decoder{DecodedTable{}};
    const ResultComparator resultComparator;
    const TextComparator textComparator;
    const CaseValidator validator{decoder, resultComparator, textComparator, catalog};
    const auto definition = testCase(ErrorExpectation{.code = ErrorCode::TestException, .message = std::nullopt});
    const FailedExecution failed{
        .id = testId(),
        .stage = ExecutionStage::Planning,
        .error
        = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = ErrorCode::TestException, .message = "expected statement failure"}}},
        .artifacts = {}};

    const auto result = validator.validate(definition, ExecutionOutcome{failed});

    EXPECT_EQ(result.verdict, Verdict::Passed);
    EXPECT_TRUE(result.diagnostics.empty());
}

}
}
