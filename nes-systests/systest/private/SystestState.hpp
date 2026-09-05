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
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/Verdict.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

class SystestRunner;


static constexpr SystestQueryId INVALID_SYSTEST_QUERY_ID = INVALID<SystestQueryId>;
static constexpr SystestQueryId INITIAL_SYSTEST_QUERY_ID = INITIAL<SystestQueryId>;

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
    std::string queryDefinition;

    struct PlanInfo
    {
        DistributedLogicalPlan queryPlan;
        std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sourcesToFilePathsAndCounts;
        /// The schema of the data written to a CSV file.
        /// It's different, for example, for the checksum sink because the schema written to the CSV is not the input schema to the sink.
        Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;

        PlanInfo() = delete;

        PlanInfo(
            DistributedLogicalPlan plan,
            std::unordered_map<SourceDescriptor, std::pair<SourceInputFile, uint64_t>> sources,
            Schema<UnqualifiedUnboundField, Ordered> sinkSchema)
            : queryPlan(std::move(plan)), sourcesToFilePathsAndCounts(std::move(sources)), sinkOutputSchema(std::move(sinkSchema))
        {
        }

        PlanInfo(DistributedLogicalPlan plan, Schema<UnqualifiedUnboundField, Ordered> sinkSchema)
            : queryPlan(std::move(plan)), sinkOutputSchema(std::move(sinkSchema))
        {
        }

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
    Expectation expectation;
    std::shared_ptr<const std::vector<std::jthread>> additionalSourceThreads;
    ConfigurationOverride configurationOverride;
    std::optional<DistributedLogicalPlan> differentialQueryPlan;
    std::optional<std::pair<TestName, SystestQueryId>> runAfter;
    std::optional<std::string> actualExplainOutput;
};

struct RunningQuery
{
    SystestQuery systestQuery;
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    std::optional<DistributedQueryId> differentialQueryPair;
    std::optional<DistributedQueryStatusSnapshot> queryStatus;
    std::optional<uint64_t> bytesProcessed{0};
    std::optional<uint64_t> tuplesProcessed{0};
    /// What the check said about this query. Empty until the runner has checked it.
    std::optional<Verdict> verdict;
    std::optional<DistributedException> exception;

    std::chrono::duration<double> getElapsedTime() const;
    [[nodiscard]] std::string getThroughput() const;
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
