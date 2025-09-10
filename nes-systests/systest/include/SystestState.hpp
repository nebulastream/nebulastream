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
#include <chrono>
#include <compare>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/base.h>
#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SystestConfiguration.hpp>

namespace NES::Systest
{

class SystestRunner;

using TestName = std::string;
using TestGroup = std::string;
using SystestQueryId = NESStrongType<uint64_t, struct SystestQueryId_, 0, 1>;
static constexpr SystestQueryId INVALID_SYSTEST_QUERY_ID = INVALID<SystestQueryId>;
static constexpr SystestQueryId INITIAL_SYSTEST_QUERY_ID = INITIAL<SystestQueryId>;

struct ExpectedError
{
    ErrorCode code;
    std::optional<std::string> message;
};

class SourceInputFile
{
public:
    using Underlying = std::filesystem::path;

    explicit constexpr SourceInputFile(Underlying value) : value(std::move(value)) { }

    friend std::ostream& operator<<(std::ostream& os, const SourceInputFile& timestamp) { return os << timestamp.value; }

    [[nodiscard]] Underlying getRawValue() const { return value; }

    friend std::strong_ordering operator<=>(const SourceInputFile& lhs, const SourceInputFile& rhs) = default;

private:
    Underlying value;
};

struct SystestQueryContext
{
    static std::filesystem::path
    resultFile(const std::filesystem::path& workingDir, std::string_view testName, SystestQueryId queryIdInTestFile);

    static std::filesystem::path sourceFile(const std::filesystem::path& workingDir, std::string_view testName, uint64_t sourceId);

    [[nodiscard]] std::filesystem::path resultFile() const;

    TestName testName;
    std::filesystem::path testFilePath;
    std::filesystem::path workingDir;
    SystestQueryId queryIdInFile = INVALID_SYSTEST_QUERY_ID;
    std::string queryDefinition;
    std::variant<std::vector<std::string>, ExpectedError> expectedResultsOrError;
    std::shared_ptr<std::vector<std::jthread>> additionalSourceThreads;
};

struct PlanInfo
{
    PlanStage::DistributedLogicalPlan plan;
    std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourcesToFilePathsAndCounts;
    Schema sinkOutputSchema;
};

struct PlannedQuery
{
    friend std::ostream& operator<<(std::ostream& os, const PlannedQuery& planned)
    {
        return os << fmt::format("[{}, systest -t {}:{}]", planned.ctx.testName, planned.ctx.testFilePath, planned.ctx.queryIdInFile);
    }

    SystestQueryContext ctx;
    std::expected<PlanInfo, Exception> planInfoOrException;
};

struct SubmittedQuery
{
    SubmittedQuery() = delete;

    SubmittedQuery(Query&& qry, PlannedQuery&& planned) /// NOLINT(*-rvalue-reference-param-not-moved)
        : query{std::move(qry)}, ctx{std::move(planned.ctx)}, planInfo{std::move(*planned.planInfoOrException)}
    {
    }

    Query query;
    SystestQueryContext ctx;
    PlanInfo planInfo;
    /// Optional benchmarking metrics
    std::optional<size_t> bytesProcessed;
    std::optional<size_t> tuplesProcessed;
};

struct FailedQuery
{
    FailedQuery() = delete;

    FailedQuery(SystestQueryContext&& ctx, std::vector<Exception>&& exceptions) : ctx{std::move(ctx)}, exceptions{{std::move(exceptions)}}
    {
    }

    friend std::ostream& operator<<(std::ostream& os, const FailedQuery& failed)
    {
        return os << fmt::format("[{}:{}, {}]", failed.ctx.testName, failed.ctx.queryIdInFile, failed.ctx.queryDefinition);
    }

    SystestQueryContext ctx;
    std::vector<Exception> exceptions;
};

struct FinishedQuery
{
    FinishedQuery() = delete;

    explicit FinishedQuery(SubmittedQuery&& query)
        : ctx{std::move(query.ctx)}, planInfo{std::move(query.planInfo)} { } /// NOLINT(*-rvalue-reference-param-not-moved)

    explicit FinishedQuery(PlannedQuery&& query) : ctx{std::move(query.ctx)} { } /// NOLINT(*-rvalue-reference-param-not-moved)

    SystestQueryContext ctx;
    /// Optional performance metrics
    std::optional<PlanInfo> planInfo;
    std::optional<std::chrono::duration<double>> elapsedTime;
    std::optional<std::string> throughput;
};

struct TestFile
{
    explicit TestFile(
        const std::filesystem::path& file, std::shared_ptr<SourceCatalog> sourceCatalog, std::shared_ptr<SinkCatalog> sinkCatalog);

    explicit TestFile(
        const std::filesystem::path& file,
        std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber,
        std::shared_ptr<SourceCatalog> sourceCatalog,
        std::shared_ptr<SinkCatalog> sinkCatalog);
    [[nodiscard]] std::string getLogFilePath() const;

    [[nodiscard]] TestName name() const { return file.stem().string(); }

    std::filesystem::path file;
    std::unordered_set<SystestQueryId> onlyEnableQueriesWithTestQueryNumber;
    std::vector<TestGroup> groups;
    std::vector<PlannedQuery> queries;
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<std::filesystem::path, TestFile>;

std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const SystestConfiguration& config);

}

FMT_OSTREAM(NES::Systest::FailedQuery);
FMT_OSTREAM(NES::Systest::PlannedQuery);
