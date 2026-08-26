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
#include <Model/ConfigurationOverride.hpp>
#include <Model/RewrittenTest.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Util/Pointers.hpp>
#include <SystestBinder.hpp>

namespace NES
{

/// Owns the workers a run submits to, and runs rewritten test files against them.
/// A worker takes its settings at startup, so the run holds one per set of settings its test files ask for.
class TestRunner
{
public:
    explicit TestRunner(const SystestConfiguration& config);
    ~TestRunner();

    /// Parses one test file and rewrites each of its parts into the SQL to submit.
    /// Throws when the file cannot be read or parsed, which the caller reports as one failed check for the file.
    [[nodiscard]] std::vector<Systest::RewrittenPart> rewrite(const DiscoveredTestFile& testfile);

    /// Receives each case as it is checked, so a caller can report it while the rest of the run continues.
    /// The timings hold one entry per statement the case submitted, in submission order.
    /// Called from the thread that runs the checks, one case at a time, so an observer needs no locking of its own.
    using QueryObserver = std::function<void(const TestCaseId&, const RewrittenCase&, const Verdict&, std::span<const QueryTiming>)>;

    /// What setting the test files up produced: the ones a run may submit, a failed check for each one it may not, and
    /// the data servers, which have to outlive every query reading from them.
    struct SetUpRun
    {
        std::vector<std::reference_wrapper<const RewrittenTest>> ready;
        std::vector<CheckedQuery> rejected;
        std::vector<std::jthread> servers;
    };

    /// Stages the data, puts the setup statements of every test file into the catalogs, and compiles its cases.
    /// Separate from submitting the queries, because a caller that submits them more than once must set up only once:
    /// a second CREATE of the same name is a catalog conflict rather than more load, and compiling again would measure
    /// the optimizer rather than the query.
    /// The settings say which worker each test file runs on, one entry per test file. They go away once a worker is
    /// registered for its settings and the statement carries the host it goes to.
    [[nodiscard]] SetUpRun setUpAll(const std::vector<RewrittenTest>& runnables, std::span<const ConfigurationOverride> settings);

    /// Submits the cases of the test files that were set up, up to `concurrency` at a time, and checks each one.
    /// A case that has to follow the one before it waits for that one to reach a terminal state.
    /// The checks come back in test file order however the cases interleave, so the report does not depend on timing.
    [[nodiscard]] std::vector<CheckedQuery> submitQueries(
        const std::vector<std::reference_wrapper<const RewrittenTest>>& runnables, size_t concurrency, const QueryObserver& observe = {});

    /// Sets every test file up, then submits their cases up to `concurrency` at a time and checks each one.
    /// A file whose setup is rejected yields one failed check and none of its cases run.
    [[nodiscard]] std::vector<CheckedQuery> runAll(
        const std::vector<RewrittenTest>& runnables,
        std::span<const ConfigurationOverride> settings,
        size_t concurrency,
        const QueryObserver& observe = {});

private:
    struct Impl;
    UniquePtr<Impl> impl;
};

}
