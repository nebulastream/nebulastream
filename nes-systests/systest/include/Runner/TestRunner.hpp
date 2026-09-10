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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <coordinator/lib.h>
#include <rust/cxx.h>

#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnableTest.hpp>
#include <Model/Verdict.hpp>
#include <Runner/Cluster.hpp>

namespace NES
{

/// The settings a run starts the coordinator and its workers with.
struct RunSettings
{
    /// The optimizer settings as keys and values, which the coordinator applies rather than the runner.
    std::unordered_map<std::string, std::string> optimizer;

    /// The workers the run places its test files on.
    Cluster::Settings cluster;

    /// How long a submission waits for its statement to reach a terminal state before the coordinator answers
    /// `QueryWaitTimeout`, so a query that never finishes fails its test instead of parking the thread that submitted it.
    /// Zero waits forever.
    std::chrono::seconds queryTimeout{0};
};

/// Owns an embedded coordinator and the workers a run registers with it, and runs rewritten test files against them.
class TestRunner
{
public:
    /// Passes the optimizer settings to the coordinator at startup, then registers the workers the run places its test files on.
    explicit TestRunner(const RunSettings& settings = {});

    /// Returns where a test file asking for these settings goes, and nothing when the run cannot give it those settings.
    [[nodiscard]] std::optional<Placement> placementFor(const ConfigurationOverride& settings) { return cluster.placementFor(settings); }

    /// Receives each case as it is checked, so a caller can report it while the rest of the run continues.
    /// The timings hold one entry per statement the case submitted, in submission order.
    /// Called from the thread that runs the checks, one case at a time, so an observer needs no locking of its own.
    using QueryObserver = std::function<void(const TestCaseId&, const RunnableCase&, const Verdict&, std::span<const QueryTiming>)>;

    /// What setting the test files up produced: the ones a run may submit, a failed check for each one it may not, and
    /// the data servers, which have to outlive every query reading from them.
    struct SetUpRun
    {
        std::vector<std::reference_wrapper<const RunnableTest>> ready;
        std::vector<CheckedQuery> rejected;
        std::vector<std::jthread> servers;
    };

    /// Stages the data and submits the setup DDL of every test file.
    /// Separate from submitting the queries, because a caller that submits them more than once must set up only once:
    /// a second CREATE of the same name is a catalog conflict rather than more load.
    [[nodiscard]] SetUpRun setUpAll(const std::vector<RunnableTest>& runnables);

    /// Submits the cases of the test files that were set up, up to `concurrency` at a time, and checks each one.
    /// A case that has to follow the one before it waits for that one to reach a terminal state.
    /// The checks come back in test file order however the cases interleave, so the report does not depend on timing.
    [[nodiscard]] std::vector<CheckedQuery> submitQueries(
        const std::vector<std::reference_wrapper<const RunnableTest>>& runnables, size_t concurrency, const QueryObserver& observe = {});

    /// Sets every test file up, then submits their cases up to `concurrency` at a time and checks each one.
    /// A file whose setup the coordinator rejects yields one failed check and none of its cases run.
    /// A case that has to follow the one before it waits for that one to reach a terminal state.
    /// The checks come back in test file order however the cases interleave, so the report does not depend on timing.
    [[nodiscard]] std::vector<CheckedQuery>
    runAll(const std::vector<RunnableTest>& runnables, size_t concurrency, const QueryObserver& observe = {});

private:
    /// Stages the data the sources read, then submits the setup DDL of one test file.
    /// Returns the servers that are still sending, which have to outlive every query reading from them.
    /// Throws on the first statement the coordinator rejects, leaving the rest of that file's setup unsubmitted.
    /// The servers it had already started stop as the throw unwinds, because nothing will read from them.
    [[nodiscard]] std::vector<std::jthread> submitSetup(const RunnableTest& runnable);

    /// The deadline for one submission, in the unit the bridge takes.
    [[nodiscard]] uint64_t timeoutMillis() const { return queryTimeout.count(); }

    rust::Box<EmbeddedCoordinator> coordinator;

    /// Declared after the coordinator, because registering the workers and reading the default host both need one.
    Cluster cluster;

    /// How long one submission waits before the coordinator gives up on it.
    std::chrono::milliseconds queryTimeout;
};

}
