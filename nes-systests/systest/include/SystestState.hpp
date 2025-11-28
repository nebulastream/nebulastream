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
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <initializer_list>
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

struct ConfigurationOverride
{
    std::unordered_map<std::string, std::string> overrideParameters;
    ConfigurationOverride() = default;

    ConfigurationOverride(std::initializer_list<std::pair<std::string_view, std::string_view>> init)
    {
        for (const auto& [key, value] : init)
        {
            overrideParameters.emplace(std::string{key}, std::string{value});
        }
    }

    std::string& operator[](std::string_view key) { return overrideParameters[std::string{key}]; }

    [[nodiscard]] const std::string& at(std::string_view key) const { return overrideParameters.at(std::string{key}); }

    bool operator==(const ConfigurationOverride& other) const = default;
    bool operator!=(const ConfigurationOverride& other) const = default;
};
}

namespace std
{
template <>
struct hash<NES::Systest::ConfigurationOverride>
{
    std::size_t operator()(const NES::Systest::ConfigurationOverride& co) const noexcept
    {
        std::size_t seed = 0;
        std::hash<std::string> hasher;
        const auto mix = [](std::size_t currentSeed, std::size_t value) noexcept
        { return currentSeed ^ (value + 0x9e3779b9 + (currentSeed << 6U) + (currentSeed >> 2U)); };

        for (const auto& [key, value] : co.overrideParameters)
        {
            seed = mix(seed, hasher(key));
            seed = mix(seed, hasher(value));
        }
        return seed;
    }
};
}

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

        PlanInfo() = delete;

        PlanInfo(LogicalPlan plan, std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sources, Schema sinkSchema)
            : queryPlan(std::move(plan)), sourcesToFilePathsAndCounts(std::move(sources)), sinkOutputSchema(std::move(sinkSchema))
        {
        }

        PlanInfo(LogicalPlan plan, Schema sinkSchema) : queryPlan(std::move(plan)), sinkOutputSchema(std::move(sinkSchema)) { }

        PlanInfo(const PlanInfo& other) : queryPlan(other.queryPlan), sinkOutputSchema(other.sinkOutputSchema)
        {
            copySourceMappingFrom(other.sourcesToFilePathsAndCounts);
        }

        PlanInfo& operator=(const PlanInfo& other)
        {
            if (this == &other)
            {
                return *this;
            }
            queryPlan = other.queryPlan;
            sinkOutputSchema = other.sinkOutputSchema;
            copySourceMappingFrom(other.sourcesToFilePathsAndCounts);
            return *this;
        }

        PlanInfo(PlanInfo&&) noexcept = default;
        PlanInfo& operator=(PlanInfo&&) noexcept = default;

    private:
        void copySourceMappingFrom(const std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>>& original)
        {
            sourcesToFilePathsAndCounts.clear();
            sourcesToFilePathsAndCounts.reserve(original.size());
            for (const auto& [descriptor, fileInfo] : original)
            {
                sourcesToFilePathsAndCounts.emplace(descriptor, fileInfo);
            }
        }
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
    LocalQueryId queryId = INVALID_LOCAL_QUERY_ID;
    std::optional<LocalQueryId> differentialQueryPair;
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
    std::vector<SystestQuery> queries;
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
};

/// intermediate representation storing all considered test files
using TestFileMap = std::unordered_map<std::filesystem::path, TestFile>;


std::ostream& operator<<(std::ostream& os, const TestFileMap& testMap);

/// load test file map objects from files defined in systest config
TestFileMap loadTestFileMap(const SystestConfiguration& config);

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
