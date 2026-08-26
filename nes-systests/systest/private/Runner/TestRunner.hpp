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

/// Sets up rewritten partitions once, then submits and checks their test cases as often as the caller asks.
/// A second setup would fail on the repeated CREATEs, and rebinding would time the optimizer.
class TestRunner
{
public:
    explicit TestRunner(const SystestConfiguration& config);
    ~TestRunner();

    /// The runner calls the observer serially, once per checked test case, so the observer needs no locking.
    using Observer = std::function<void(const ReportEntry&, const RewrittenTestCase&, std::span<const StatementTiming>)>;

    /// Stages data, writes setup statements into the catalogs, and binds test cases.
    /// Returns the entries of the files whose setup failed.
    [[nodiscard]] std::vector<ReportEntry> setUpAll(std::vector<RunnablePartition> partitions);

    /// Counts only the test cases of partitions that the last setup accepted.
    [[nodiscard]] size_t countTestCases() const;

    /// Submits the test cases of the set-up files, up to `concurrency` at a time, and checks each one.
    /// Returns the entries in file order, so the report does not depend on timing.
    [[nodiscard]] std::vector<ReportEntry> submitAll(size_t concurrency, const Observer& observe = {});

private:
    struct Impl;
    UniquePtr<Impl> impl;
};

}
