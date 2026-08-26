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

#include <expected>
#include <string>
#include <variant>
#include <vector>

#include <Config/Config.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/Verdict.hpp>
#include <Rewriter/TestFileRewriter.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// The outcome of one systest invocation.
/// Both outcomes print a report, and only a failure has an error code.
/// Two types rather than one type with an optional code, so a caller cannot read a code that is not there.
struct RunSucceeded
{
    std::string report;
};

struct RunFailed
{
    std::string report;
    ErrorCode errorCode;
};

using ExecutorResult = std::variant<RunSucceeded, RunFailed>;

struct DiscoveredTestFile;
struct RunPolicy;
class TestFileRewriter;
class TestRunner;

/// Runs every discovered test file and emits status reports for each test case.
class Executor
{
public:
    explicit Executor(SystestConfiguration config);
    [[nodiscard]] ExecutorResult execute();

private:
    struct PreparedRun
    {
        std::vector<RunnablePartition> runnablePartitions;
        std::vector<ReportEntry> failedTestFiles;
    };

    /// Prepares a single test file for execution (parsing and rewriting).
    /// A file that cannot be read, parsed, or rewritten yields a failed check (and does not end the run).
    [[nodiscard]] std::expected<std::vector<RunnablePartition>, ReportEntry> prepare(const DiscoveredTestFile& discoveredTestFile);

    /// Prepares every discovered test file before any query runs.
    [[nodiscard]] PreparedRun prepareAll();

    /// Submits each test case exactly once.
    [[nodiscard]] static ExecutorResult runOnce(TestRunner& runner, const RunPolicy& plan, PreparedRun prepared);

    /// Submits the test cases in rounds, as the plan states.
    /// Setting up happens once, because for another run we can resubmit already prepared plans to the worker.
    [[nodiscard]] static ExecutorResult runRounds(TestRunner& runner, const RunPolicy& plan, const PreparedRun& prepared);

    /// An invocation that checked nothing fails, because this is likely not what the user wants (executing nothing).
    [[nodiscard]] static ExecutorResult summarize(const std::vector<ReportEntry>& checkedCases);

    SystestConfiguration config;
    TestFileRewriter rewriter;
};

}
