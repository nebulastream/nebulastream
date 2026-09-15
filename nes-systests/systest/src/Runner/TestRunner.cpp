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

#include <Runner/TestRunner.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <folly/ScopeGuard.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>
#include <BridgeError.hpp>

#include <Config/Config.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Runner/Cluster.hpp>
#include <Runner/DataStaging.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

std::chrono::nanoseconds extractExecutionTime(const Bridge::StatementOutcome& outcome)
{
    return outcome.stop_ms > outcome.start_ms ? std::chrono::milliseconds{outcome.stop_ms - outcome.start_ms}
                                              : std::chrono::nanoseconds::zero();
}

TestCaseId createIdOf(const RunnablePartition& partition, const RewrittenTestCase& testCase)
{
    return TestCaseId{.originFile = partition.file.name, .queryIdInFile = getTestCaseNumber(testCase), .overrides = partition.overrides};
}

/// One skip entry per test case of the file, so the report keeps its total.
void appendSkipped(std::vector<ReportEntry>& rejected, const RunnablePartition& partition, const std::string& reason)
{
    for (const auto& testCase : partition.file.testCases)
    {
        rejected.push_back(ReportEntry{.id = createIdOf(partition, testCase), .outcome = Skipped{.reason = reason}});
    }
}

}

TestRunner::TestRunner(const SystestConfiguration& config, const Cluster& cluster, std::vector<RunnablePartition> partitions)
    : cluster{cluster}, queryTimeout{std::chrono::seconds{config.queryTimeoutSeconds}}
{
    setUp(std::move(partitions));
}

Bridge::StatementOutcome TestRunner::submit(const std::string_view sql, const Bridge::WaitMode wait) const
{
    return cluster.get().getCoordinator().submit(
        rust::Str{sql.data(), sql.size()}, wait, static_cast<uint64_t>(queryTimeout.count()), false);
}

Bridge::StatementOutcome TestRunner::runQuery(const std::string_view sql) const
{
    auto outcome = submit(sql, Bridge::WaitMode::UntilCompleted);
    if (outcome.error.code == static_cast<uint16_t>(ErrorCode::QueryWaitTimeout) and outcome.query_id != 0)
    {
        if (const auto dropped = submit(fmt::format("DROP QUERY WHERE ID = {}", outcome.query_id), Bridge::WaitMode::None);
            not Bridge::isOk(dropped.error))
        {
            NES_WARNING("could not stop query {} after its timeout: {}", outcome.query_id, std::string{dropped.error.msg});
        }
    }
    return outcome;
}

void TestRunner::setUp(std::vector<RunnablePartition> partitions)
{
    setUpPartitions.reserve(partitions.size());
    for (auto& partition : partitions)
    {
        const auto& [overrides, runnable] = partition;
        if (not overrides.empty() and not cluster.get().runsInThisProcess())
        {
            appendSkipped(rejected, partition, "a remote run cannot apply the settings of this file to workers that are already running");
            continue;
        }
        if (not cluster.get().runsInThisProcess()
            and std::ranges::any_of(
                runnable.setupStatements,
                [](const SetupStatement& setup) { return std::holds_alternative<StatementWithServedData>(setup); }))
        {
            appendSkipped(rejected, partition, "a remote worker cannot reach a source that this process serves over a socket");
            continue;
        }
        try
        {
            auto [setupSql, staged] = stage(runnable);
            for (const auto& sql : setupSql)
            {
                if (const auto outcome = submit(sql, Bridge::WaitMode::None); not Bridge::isOk(outcome.error))
                {
                    throw TestException("setup statement failed: {}: {}", sql, std::string{outcome.error.msg});
                }
            }
            std::ranges::move(staged, std::back_inserter(servers));
            setUpPartitions.push_back(std::move(partition));
        }
        catch (const std::exception& exception)
        {
            rejected.push_back(createFailedFileEntry(runnable.name, overrides, "could not run", exception));
            appendSkipped(rejected, partition, "the file's setup failed");
        }
    }
}

const std::vector<ReportEntry>& TestRunner::getRejected() const
{
    return rejected;
}

size_t TestRunner::countTestCases() const
{
    return std::ranges::fold_left(
        setUpPartitions,
        size_t{0},
        [](const size_t sum, const RunnablePartition& partition) { return sum + partition.file.testCases.size(); });
}

std::vector<ReportEntry> TestRunner::submitAll(const size_t concurrency, const Observer& observe)
{
    std::vector<Job> jobs;
    for (const auto& partition : setUpPartitions)
    {
        for (const auto& testCase : partition.file.testCases)
        {
            jobs.push_back(Job{.partition = partition, .testCase = testCase, .reportSlot = jobs.size()});
        }
    }

    const auto threads = std::max(concurrency, size_t{1});
    folly::MPMCQueue<const Job*> toSubmit{jobs.size() + threads};
    folly::MPMCQueue<Submitted> answered{jobs.size() + threads};

    const auto nextJob = [&toSubmit]
    {
        const Job* job = nullptr;
        toSubmit.blockingRead(job);
        return job;
    };
    const auto submitJobs = [&]
    {
        while (const auto* job = nextJob())
        {
            Submitted answer{.job = job, .outcomes = {}};
            std::visit(
                Overloaded{
                    [&](const RewrittenQuery& query) { answer.outcomes.push_back(runQuery(query.sql)); },
                    [&](const RewrittenDifferential& block)
                    {
                        answer.outcomes.push_back(runQuery(block.firstSql));
                        if (Bridge::isOk(answer.outcomes.back().error))
                        {
                            answer.outcomes.push_back(runQuery(block.secondSql));
                        }
                    },
                    [&](const RewrittenExplain& explain) { answer.outcomes.push_back(submit(explain.sql, Bridge::WaitMode::None)); }},
                job->testCase.action);
            answered.blockingWrite(std::move(answer));
        }
    };
    std::vector<std::jthread> submitters;
    submitters.reserve(threads);
    SCOPE_EXIT
    {
        const Job* skipped = nullptr;
        while (toSubmit.read(skipped))
        {
        }
        std::ranges::for_each(submitters, [&](const auto&) { toSubmit.blockingWrite(nullptr); });
    };
    while (submitters.size() < threads)
    {
        submitters.emplace_back(submitJobs);
    }

    /// A test case is handed out only after an earlier one is checked.
    /// A measuring run has one submitter, so none of its queries runs while a result is compared, which would distort its time.
    auto next = jobs.begin();
    const auto handOutNext = [&]
    {
        if (next != jobs.end())
        {
            toSubmit.blockingWrite(&*next++);
        }
    };
    for (size_t handedOut = 0; handedOut < threads; ++handedOut)
    {
        handOutNext();
    }
    std::vector<std::optional<ReportEntry>> checked(jobs.size());
    for (size_t reported = 0; reported < jobs.size(); ++reported)
    {
        Submitted answer;
        answered.blockingRead(answer);
        const auto& [partition, testCase, reportSlot] = *answer.job;
        ReportEntry entry{
            .id = createIdOf(partition, testCase), .outcome = checkTestCase(answer.outcomes, testCase, partition.file.originalNames)};
        if (observe)
        {
            const auto timings = answer.outcomes
                | std::views::transform([](const Bridge::StatementOutcome& outcome)
                                        { return StatementTiming{.execution = extractExecutionTime(outcome)}; })
                | std::ranges::to<std::vector>();
            observe(entry, testCase, timings);
        }
        checked.at(reportSlot) = std::move(entry);
        handOutNext();
    }
    return checked
        | std::views::transform(
               [](std::optional<ReportEntry>& entry)
               {
                   INVARIANT(entry.has_value(), "every submitted test case was checked");
                   return std::move(*entry);
               })
        | std::ranges::to<std::vector>();
}

}
