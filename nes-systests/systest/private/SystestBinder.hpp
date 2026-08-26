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
#include <Discovery/TestDiscovery.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// One test file partition, rewritten and ready to bind, together with the worker settings that it asks for.
/// The settings stay with the partition because a worker takes them at startup, so the partition runs on the worker that has them.
struct RewrittenPart
{
    ConfigurationOverride settings;
    RunnableTestFile test;
};

/// A plan to submit, and the schema its sink writes.
/// The schema is what the result file is read back with, which differs from the sink's input schema for a checksum sink.
struct PlanInfo
{
    DistributedLogicalPlan plan;
    Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;
};

/// One statement of a test case, bound.
/// Either a plan to submit, or what stopped it from binding, which a test that expects an error compares against.
/// An EXPLAIN is answered while binding, because only this component holds the optimizer that the printed stages need,
/// so it holds its printed plan and has none to run.
struct PlannedStatement
{
    std::expected<PlanInfo, Exception> plan;
    std::optional<std::string> explained;
};

/// One test file partition, bound:
/// its data staged, its setup statements in the catalogs, and its test cases ready to submit.
/// The servers have to outlive every query that reads from them.
struct PlannedTest
{
    /// One entry per test case, holding one bound statement for a query and two for a differential block.
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

    /// Parses one test file, splits it by the worker settings that its queries ask for, and rewrites each partition.
    /// Throws when the file cannot be read or parsed.
    [[nodiscard]] std::vector<RewrittenPart> rewrite(const DiscoveredTestFile& testfile);

    /// Stages the data that this file's sources read, writes its setup statements into the shared catalogs, and binds its test cases.
    /// Throws on the first setup statement that the catalogs reject, because a statement below it may need the one that failed.
    /// A test case that does not bind is reported per test case rather than thrown, because a test may expect that error.
    [[nodiscard]] PlannedTest bind(const RunnableTestFile& runnable);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
