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

#include <cstddef>
#include <functional>
#include <span>
#include <thread>
#include <vector>

#include <Config/Config.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

/// Owns the workers that a run submits to, and runs rewritten test files against them.
/// A worker takes its settings at startup, so the run holds one worker per set of settings that its test files ask for.
class TestRunner
{
public:
    explicit TestRunner(const SystestConfiguration& config);
    ~TestRunner();

    /// Receives each test case as it is checked, so a caller can report it while the rest of the run continues.
    /// The timings hold one entry per submitted statement, in submission order.
    /// Called from the thread that runs the checks, one test case at a time, so an observer needs no locking of its own.
    using QueryObserver = std::function<void(const TestCaseId&, const RewrittenTestCase&, const Verdict&, std::span<const QueryTiming>)>;

    /// What setting the test files up produced: the ones that a run may submit, a failed check for each one that it may not,
    /// and the data servers, which have to outlive every query that reads from them.
    struct SetUpRun
    {
        std::vector<std::reference_wrapper<const RunnableTestFile>> ready;
        std::vector<ReportEntry> rejected;
        std::vector<std::jthread> servers;
    };

    /// Stages the data, puts the setup statements of every test file into the catalogs, and binds its test cases.
    /// Separate from submitting the queries, because a caller that submits them more than once must set up only once:
    /// a second CREATE of the same name is a catalog conflict rather than more load, and binding again would measure
    /// the optimizer rather than the query.
    [[nodiscard]] SetUpRun setUpAll(const std::vector<RunnablePartition>& partitions);

    /// Submits the test cases of the test files that were set up, up to `concurrency` at a time, and checks each one.
    /// The checks come back in test file order however the test cases interleave, so the report does not depend on timing.
    [[nodiscard]] std::vector<ReportEntry> submitQueries(
        const std::vector<std::reference_wrapper<const RunnableTestFile>>& runnables,
        size_t concurrency,
        const QueryObserver& observe = {});

    /// Sets every test file up, then submits their test cases up to `concurrency` at a time and checks each one.
    /// A file whose setup is rejected yields one failed check and none of its test cases run.
    [[nodiscard]] std::vector<ReportEntry>
    runAll(const std::vector<RunnablePartition>& partitions, size_t concurrency, const QueryObserver& observe = {});

private:
    struct Impl;
    UniquePtr<Impl> impl;
};

}
