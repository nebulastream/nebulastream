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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>
#include <SystestIdentifiers.hpp>

namespace NES::Systest
{

using ResultSchema = Schema<UnqualifiedUnboundField, Ordered>;
using ResultRow = std::string;

struct Origin
{
    std::filesystem::path file;
    size_t firstLine = 0;
    size_t lastLine = 0;

    bool operator==(const Origin&) const = default;
};

enum class QueryKind : uint8_t
{
    Execute,
    Explain
};

struct QueryAction
{
    std::string sql;
    QueryKind kind = QueryKind::Execute;

    bool operator==(const QueryAction&) const = default;
};

struct DifferentialAction
{
    std::string leftSql;
    std::string rightSql;

    bool operator==(const DifferentialAction&) const = default;
};

using CaseAction = std::variant<QueryAction, DifferentialAction>;

enum class ComparisonPolicy : uint8_t
{
    UnorderedTypedRows
};

struct RowsExpectation
{
    std::vector<std::string> rows;
    ComparisonPolicy comparison = ComparisonPolicy::UnorderedTypedRows;
};

struct ErrorExpectation
{
    ErrorCode code = ErrorCode::UnknownException;
    std::optional<std::string> message;
};

enum class TextMatchPolicy : uint8_t
{
    Automatic,
    Exact,
    RegexAssertions
};

struct TextExpectation
{
    std::vector<std::string> lines;
    TextMatchPolicy matching = TextMatchPolicy::Automatic;
};

struct DifferentialExpectation
{
};

using CaseExpectation = std::variant<RowsExpectation, ErrorExpectation, TextExpectation, DifferentialExpectation>;

struct ConfigurationDirective
{
    std::string key;
    std::vector<std::string> values;
    bool global = false;
    Origin source;
};

struct InlineSourceData
{
    std::vector<std::string> rows;
};

struct FileSourceData
{
    std::filesystem::path file;
};

using SourceDataSpec = std::variant<InlineSourceData, FileSourceData>;

struct FixtureStatement
{
    std::string sql;
    std::optional<SourceDataSpec> attachment;
    Origin source;
};

struct ParsedCase
{
    CaseKey key;
    Origin source;
    CaseAction action;
    CaseExpectation expectation;
    std::optional<CaseKey> runAfter;
    std::vector<ConfigurationDirective> configuration;
};

struct ParsedTestFile
{
    std::filesystem::path file;
    std::filesystem::path relativeTestFile;
    std::vector<FixtureStatement> fixtures;
    std::vector<ParsedCase> cases;
};

struct EffectiveConfiguration
{
    std::vector<std::pair<std::string, std::string>> values;

    auto operator<=>(const EffectiveConfiguration&) const = default;
};

struct EnvironmentSpec
{
    EnvironmentId id;
    std::filesystem::path relativeTestFile;
    std::vector<FixtureStatement> setupStatements;
    EffectiveConfiguration configuration;
    SystestClusterConfiguration cluster;
};

struct ResolvedCase
{
    TestCaseId id;
    EnvironmentId environment;
    Origin source;
    CaseAction action;
    CaseExpectation expectation;
    std::vector<TestCaseId> dependencies;
};

struct ScheduledCase
{
    std::shared_ptr<const ResolvedCase> definition;
    uint64_t sequenceNumber = 0;
};

enum class OutputTargetKind : uint8_t
{
    Table,
    Text,
    Discard
};

struct OutputTarget
{
    OutputTargetKind kind = OutputTargetKind::Table;
    std::filesystem::path file;

    bool operator==(const OutputTarget&) const = default;
};

struct TableArtifact
{
    std::filesystem::path file;
};

struct TextArtifact
{
    std::string text;
};

struct DiscardedOutput
{
};

using StatementOutput = std::variant<TableArtifact, TextArtifact, DiscardedOutput>;

struct ExecutionMetrics
{
    std::optional<std::chrono::system_clock::time_point> started;
    std::optional<std::chrono::system_clock::time_point> finished;
    uint64_t bytesProcessed = 0;
    uint64_t tuplesProcessed = 0;

    [[nodiscard]] std::chrono::duration<double> elapsed() const
    {
        if (!started || !finished)
        {
            return {};
        }
        return std::chrono::duration_cast<std::chrono::duration<double>>(*finished - *started);
    }
};

struct ArtifactSet
{
    std::vector<std::filesystem::path> files;
};

enum class ExecutionStage : uint8_t
{
    Planning,
    Starting,
    Running,
    Cancelling,
    Closing
};

struct ExecutionErrorDetail
{
    ErrorCode code = ErrorCode::UnknownException;
    std::string message;
};

enum class ExecutionErrorKind : uint8_t
{
    Statement,
    Backend
};

struct ExecutionError
{
    ExecutionErrorKind kind = ExecutionErrorKind::Statement;
    std::vector<ExecutionErrorDetail> details;

    [[nodiscard]] bool contains(const ErrorCode code) const
    {
        for (const auto& detail : details)
        {
            if (detail.code == code)
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] std::string message() const;
};

struct CompletedExecution
{
    TestCaseId id;
    std::vector<StatementOutput> outputs;
    ExecutionMetrics metrics;
    ArtifactSet artifacts;
};

struct FailedExecution
{
    TestCaseId id;
    ExecutionStage stage = ExecutionStage::Running;
    ExecutionError error;
    ArtifactSet artifacts;
};

struct TimedOutExecution
{
    TestCaseId id;
    ExecutionStage stage = ExecutionStage::Running;
    std::chrono::milliseconds elapsed{};
    ArtifactSet artifacts;
};

struct SkippedExecution
{
    TestCaseId id;
    std::vector<TestCaseId> failedDependencies;
    std::optional<std::string> reason;
};

using ExecutionOutcome = std::variant<CompletedExecution, FailedExecution, TimedOutExecution, SkippedExecution>;

enum class Verdict : uint8_t
{
    Passed,
    Failed,
    Skipped
};

enum class DiagnosticKind : uint8_t
{
    Validation,
    Execution,
    Scheduling,
    Reporting
};

struct Diagnostic
{
    DiagnosticKind kind = DiagnosticKind::Validation;
    std::string message;
    std::optional<Origin> source;
};

using ValidationDiagnostic = Diagnostic;

struct ValidatedResult
{
    TestCaseId id;
    Verdict verdict = Verdict::Failed;
    std::vector<Diagnostic> diagnostics;
    ExecutionMetrics metrics;
    ArtifactSet artifacts;
};

struct DecodedInputStream
{
    ResultSchema schema;
    std::vector<ResultRow> rows;
};

using DecodedTable = DecodedInputStream;

struct ComparisonResult
{
    bool matches = false;
    std::vector<Diagnostic> diagnostics;
};

}
