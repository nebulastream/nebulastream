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
#include <Model/ConfigurationOverride.hpp>
#include <Model/RewrittenTest.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{

/// One test file part, rewritten and ready to compile, together with the worker settings it asks for.
/// The settings travel next to the part because a worker takes them at startup, so the part runs on the worker that has them.
struct RewrittenPart
{
    ConfigurationOverride settings;
    RewrittenTest test;
};

/// A plan to submit, and the schema its sink writes.
/// The schema is what the result file is read back with, which differs from the sink's input schema for a checksum sink.
struct PlanInfo
{
    DistributedLogicalPlan plan;
    Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;
};

/// One statement of a case, compiled.
/// Either a plan to submit, or what stopped it from compiling, which a test expecting an error compares against.
/// An EXPLAIN is answered while compiling, because only this component holds the optimizer that the printed stages
/// need, so it carries its printed plan and has none to run.
struct PlannedStatement
{
    std::expected<PlanInfo, Exception> plan;
    std::optional<std::string> explained;
};

/// One test file partition, planned:
/// its data staged, its setup statements in the catalogs, and its cases ready to submit.
/// The servers have to outlive every query reading from them.
struct PlannedTest
{
    /// One entry per case, holding one compiled statement for a query and two for a differential block.
    std::vector<std::vector<PlannedStatement>> cases;
    std::vector<std::jthread> servers;
};

/// The plan compiler that is invoked between the rewriter and the runner.
/// Rewriting a test file and compiling it are separate, because the run rewrites every file before it runs any query,
/// and a part is compiled only once its setup can go into the catalogs.
class SystestBinder
{
public:
    explicit SystestBinder(const SystestConfiguration& config);

    /// Parses one test file, splits it by potentially multiple different worker settings, and rewrites each partition.
    /// Throws when the file cannot be read or parsed.
    [[nodiscard]] std::vector<RewrittenPart> rewrite(const DiscoveredTestFile& testfile);

    /// Stages the data read by the sources in this file, writes its setup statements into the shared catalogs, and plans its test cases.
    /// Throws on the first setup statement the catalogs reject, because a statement below it potentially needs the one that failed.
    /// A case that does not compile is reported per case rather than thrown, because a test may expect that error.
    [[nodiscard]] PlannedTest plan(const RewrittenTest& runnable);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
