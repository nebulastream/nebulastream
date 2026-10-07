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
#include <vector>

#include <Config/Config.hpp>
#include <Model/RunnablePartition.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <Util/Pointers.hpp>

namespace NES
{

/// Prepares rewritten partitions when constructed, then submits and checks their test cases.
/// Each submission runs every bound test case once more, so a caller repeats a run by submitting again.
/// A second preparation would fail on the repeated CREATEs, so only submit them once against the catalog.
class TestRunner
{
public:
    /// Stages data, writes setup statements into the catalogs, and binds test cases.
    /// A file whose preparation fails is reported rather than thrown, so the other files still run.
    TestRunner(const SystestConfiguration& config, std::vector<RunnablePartition> partitions);
    ~TestRunner();

    TestRunner(const TestRunner&) = delete;
    TestRunner(TestRunner&&) = delete;
    TestRunner& operator=(const TestRunner&) = delete;
    TestRunner& operator=(TestRunner&&) = delete;

    /// Called for each test case after it has been checked.
    /// Enables the caller to report progress and record timings for reporting.
    /// The runner calls the observer serially, once per checked test case, so the observer needs no locking.
    using Observer = std::function<void(const ReportEntry&, const RewrittenTestCase&, std::span<const StatementTiming>)>;

    /// The entries of the files whose preparation failed: one for the failure and one skip per test case.
    [[nodiscard]] const std::vector<ReportEntry>& getRejected() const;

    /// Counts only the test cases of the partitions that preparation accepted.
    [[nodiscard]] size_t countTestCases() const;

    /// Submits the test cases of the prepared files, up to `concurrency` at a time, and checks each one.
    /// Returns the entries in file order, so the report does not depend on timing.
    [[nodiscard]] std::vector<ReportEntry> submitAll(size_t concurrency, const Observer& observe = {});

private:
    struct Impl;
    UniquePtr<Impl> impl;
};

}
