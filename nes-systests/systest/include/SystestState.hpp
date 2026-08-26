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
#include <Identifiers/NESStrongType.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/Expectation.hpp>
#include <Model/SystestQueryId.hpp>
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

using TestName = std::string;
using TestGroup = std::string;

static constexpr SystestQueryId INVALID_SYSTEST_QUERY_ID = INVALID<SystestQueryId>;
static constexpr SystestQueryId INITIAL_SYSTEST_QUERY_ID = INITIAL<SystestQueryId>;

struct SystestQuery
{
    TestName testName;
    SystestQueryId queryIdInFile = INVALID_SYSTEST_QUERY_ID;
    std::filesystem::path testFilePath;
    std::string queryDefinition;

    struct PlanInfo
    {
        DistributedLogicalPlan queryPlan;
        /// The schema of the data written to a CSV file.
        /// It's different, for example, for the checksum sink because the schema written to the CSV is not the input schema to the sink.
        Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;

        PlanInfo() = delete;

        PlanInfo(DistributedLogicalPlan plan, Schema<UnqualifiedUnboundField, Ordered> sinkSchema)
            : queryPlan(std::move(plan)), sinkOutputSchema(std::move(sinkSchema))
        {
        }
    };

    std::expected<PlanInfo, Exception> planInfoOrException;
    Expectation expectation;
    std::shared_ptr<const std::vector<std::jthread>> additionalSourceThreads;
    ConfigurationOverride configurationOverride;
    std::optional<DistributedLogicalPlan> differentialQueryPlan;
    std::optional<std::pair<TestName, SystestQueryId>> runAfter;
    std::optional<std::string> actualExplainOutput;
    /// The file that the query's sink writes, which the rewriter chose when it inlined the sink.
    /// Absent when the query writes none, such as a query into a discarding sink, and then there is nothing to check.
    std::optional<std::filesystem::path> resultFile;
    /// The second result file of a differential pair, which the comparison reads against the first.
    std::optional<std::filesystem::path> differentialResultFile;
    /// The data files that the query's sources read, once per reference, which a measurement derives throughput from.
    std::vector<std::filesystem::path> inputFiles;
};

struct RunningQuery
{
    SystestQuery systestQuery;
    DistributedQueryId queryId{DistributedQueryId::INVALID};
    std::optional<DistributedQueryId> differentialQueryPair;
    std::optional<DistributedQueryStatusSnapshot> queryStatus;
    std::optional<uint64_t> bytesProcessed{0};
    std::optional<uint64_t> tuplesProcessed{0};
    /// What the check said about this query, which the runner fills in once the query has been checked.
    Verdict verdict;
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
