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
#include <atomic>
#include <chrono>
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

#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/base.h>
#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Util/Logger/Formatter.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <SystestConfiguration.hpp>

#include <Identifiers/NESStrongType.hpp>

namespace NES::Systest
{


class SystestRunner;

using TestName = std::string;
using TestGroup = std::string;

using SystestQueryId = NESStrongType<uint64_t, struct SystestQueryId_, 0, 1>;
static constexpr SystestQueryId INVALID_SYSTEST_QUERY_ID = INVALID<SystestQueryId>;
static constexpr SystestQueryId INITIAL_SYSTEST_QUERY_ID = INITIAL<SystestQueryId>;

using ConfigurationOverride = std::unordered_map<std::string, std::string>;

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

struct SystestQuery
{
    static std::filesystem::path
    resultFile(const std::filesystem::path& workingDir, std::string_view testName, SystestQueryId queryIdInTestFile);

    static std::filesystem::path sourceFile(const std::filesystem::path& workingDir, std::string_view testName, uint64_t sourceId);
    [[nodiscard]] std::filesystem::path resultFile() const;
    [[nodiscard]] std::filesystem::path resultFileForDifferentialQuery() const;

    TestName testName;
    SystestQueryId queryIdInFile = INVALID_SYSTEST_QUERY_ID;
    std::filesystem::path testFilePath;
    std::filesystem::path workingDir;
    /// The schema of the data written to a CSV file.
    /// It's different, for example, for the checksum sink because the schema written to the CSV is not the input schema to the sink.
    std::string queryDefinition;

    struct PlanInfo
    {
        LogicalPlan queryPlan;
        std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourcesToFilePathsAndCounts;
        Schema sinkOutputSchema;
    };

    std::expected<PlanInfo, Exception> planInfoOrException;
    std::variant<std::vector<std::string>, ExpectedError> expectedResultsOrExpectedError;
    std::shared_ptr<const std::vector<std::jthread>> additionalSourceThreads;
    ConfigurationOverride configurationOverride;
    std::optional<LogicalPlan> differentialQueryPlan;
};

struct RunningQuery
{
    SystestQuery systestQuery;
    QueryId queryId = INVALID_QUERY_ID;
    std::optional<QueryId> differentialQueryPair;
    LocalQueryStatus queryStatus{};
    std::optional<uint64_t> bytesProcessed{0};
    std::optional<uint64_t> tuplesProcessed{0};
    bool passed = false;
    std::optional<Exception> exception;

    std::chrono::duration<double> getElapsedTime() const;
    [[nodiscard]] std::string getThroughput() const;
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
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<std::filesystem::path, TestFile>;


std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const SystestConfiguration& config);

class SystestProgressTracker
{
public:
    SystestProgressTracker();
    explicit SystestProgressTracker(size_t totalQueries);

    void incrementQueryCounter();
    [[nodiscard]] size_t getQueryCounter() const;
    void setTotalQueries(size_t total);

    [[nodiscard]] size_t getTotalQueries() const;

    [[nodiscard]] double getProgress() const;

    void reset();
    void reset(size_t newTotalQueries);

private:
    std::atomic<size_t> queryCounter{0};
    size_t totalQueries{0};
};

}

template <>
struct fmt::formatter<NES::Systest::RunningQuery> : formatter<std::string>
{
    static constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

    static auto format(const NES::Systest::RunningQuery& runningQuery, format_context& ctx) -> decltype(ctx.out())
    {
        return fmt::format_to(
            ctx.out(),
            "[{}, systest -t {}:{}]",
            runningQuery.systestQuery.testName,
            runningQuery.systestQuery.testFilePath,
            runningQuery.systestQuery.queryIdInFile);
    }
};

namespace std
{
template <>
struct hash<NES::Systest::ConfigurationOverride>
{
    std::size_t operator()(const NES::Systest::ConfigurationOverride& cfg) const noexcept
    {
        std::size_t seed = 0;
        const auto hasher = std::hash<std::string>{};

        for (const auto& [k, v] : cfg)
        {
            seed ^= hasher(k) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};
}
