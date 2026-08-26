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
#include <span>
#include <string>
#include <vector>

#include <Config/Config.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Pointers.hpp>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

struct BoundPlan
{
    DistributedLogicalPlan plan;
    Schema<UnqualifiedUnboundField, Ordered> sinkOutputSchema;
};

struct BoundStatement
{
    std::expected<BoundPlan, Exception> plan;
    /// An EXPLAIN's output, computed while binding in place of a runnable plan.
    std::optional<std::string> explained;
};

struct BoundTestFile
{
    /// One entry per test case, each with one bound statement for a query and two for a differential block.
    std::vector<std::vector<BoundStatement>> testCases;
};

/// Binds rewritten statements into plans, between the rewriter and the runner.
/// Runs after rewriting: binding needs the partition's setup in the catalogs.
class SystestBinder
{
public:
    explicit SystestBinder(const SystestConfiguration& config);

    /// Writes the staged setup statements into the shared catalogs and binds the file's test cases.
    /// Throws on the first setup statement that the catalogs reject, because other statements depend on it.
    /// The statements before the rejected one stay in the catalogs.
    /// Other partitions can still be bound afterwards, because the rewriter prefixes each partition's names.
    /// An unbindable test case yields an error entry, not a throw: a test may expect that error.
    [[nodiscard]] BoundTestFile
    bind(std::span<const std::string> setupSql, std::span<const RewrittenTestCase> testCases, const std::string& partitionKey);

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
