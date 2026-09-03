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
#include <Model/RunnableTest.hpp>
#include <Model/Verdict.hpp>
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
struct RunPlan;
class TestRunner;

/// Runs every discovered test file against one shared coordinator and counts the queries that passed.
class Executor
{
public:
    explicit Executor(Config config);
    [[nodiscard]] ExecutorResult execute() const;

private:
    /// What preparing the discovered test files produced: the ones ready to run, and a failed check for each one that is not.
    struct PreparedRun
    {
        std::vector<RunnableTest> ready;
        std::vector<CheckedQuery> unprepared;
    };

    /// Reads one test file and rewrites it into the SQL to submit.
    /// A file that cannot be read or rewritten yields one failed check, so one unusable file does not end the run.
    /// A file yields several parts when its queries ask for different worker settings, because a worker takes its settings at startup.
    [[nodiscard]] std::expected<std::vector<RunnableTest>, CheckedQuery>
    prepare(const DiscoveredTestFile& discovered, TestRunner& runner) const;

    /// Prepares every discovered test file before any query runs, so progress can count towards a known total.
    [[nodiscard]] PreparedRun prepareAll(TestRunner& runner) const;

    /// Runs every case one time, reporting each as it finishes.
    /// Files that could not be prepared or set up join the report as failures next to the cases that ran.
    [[nodiscard]] static ExecutorResult
    runOnce(TestRunner& runner, const RunPlan& plan, const std::vector<RunnableTest>& prepared, std::vector<CheckedQuery> unprepared);

    /// Submits the cases round after round, to keep a worker under load or to measure each query.
    /// Setting up happens once, because a second CREATE of the same name is a catalog conflict rather than more load.
    /// Refuses files that could not be prepared or set up, because they are failures of the invocation rather than
    /// something to repeat, and stops on the first round that fails or once the plan's limits are reached.
    [[nodiscard]] static ExecutorResult
    runRounds(TestRunner& runner, const RunPlan& plan, const std::vector<RunnableTest>& prepared, std::vector<CheckedQuery> unprepared);

    /// Turns the checked queries into the tally and failure list the invocation prints.
    /// An invocation that checked nothing fails, because selecting no query is not the same as passing.
    [[nodiscard]] static ExecutorResult summarize(const std::vector<CheckedQuery>& checked);

    Config config;
};
}
