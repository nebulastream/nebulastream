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
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <Config/Config.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct PlanInfo
{
    DistributedLogicalPlan plan;
    Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;
};

struct PlannedStatement
{
    std::expected<PlanInfo, Exception> plan;
    /// An EXPLAIN is invoked during binding, so it holds its printed plan and has no plan to run.
    std::optional<std::string> explained;
};

struct PlannedTest
{
    /// One entry per test case, each with one bound statement for a query and two for a differential block.
    std::vector<std::vector<PlannedStatement>> testCases;
    std::vector<std::jthread> servers;
};

/// Binds rewritten statements into plans, between the rewriter and the runner.
/// Rewriting a test file and binding it are separate, because the run rewrites every file before it runs any query,
/// and a partition is bound only once its setup can go into the catalogs.
class SystestBinder
{
public:
    explicit SystestBinder(const SystestConfiguration& config);

    /// 1. Stages the data that this test file's sources read
    /// 2. Writes setup statements into the shared catalogs
    /// 3. Binds its test cases
    /// Throws on the first setup statement that the catalogs reject, because other statements depend on it.
    /// A case that does not bind is reported per test case, not thrown, because a test may expect that error.
    [[nodiscard]] PlannedTest bind(const RunnableTestFile& runnable);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
