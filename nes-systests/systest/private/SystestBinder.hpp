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

/// One partition's statements to bind: its staged setup and the test cases that run against it.
struct PartitionToBind
{
    std::span<const std::string> setupSql;
    std::span<const RewrittenTestCase> testCases;
    /// Goes into every query id, because partitions repeat query numbers and the coordinator rejects duplicate ids.
    std::string partitionKey;
};

/// A partition's bound test cases, or the setup statement that the catalogs rejected.
using BindResult = std::expected<BoundTestFile, Exception>;

/// Binds the rewritten statements of one run into plans, between the rewriter and the runner.
/// Single shot: constructing binds every partition, in order, against one shared catalog set, and the results stay on
/// the binder. There is no second call, so no caller can observe the catalogs between two partitions.
class SystestBinder
{
public:
    /// Writes each partition's setup statements into the catalogs and binds its test cases.
    /// A setup statement that the catalogs reject fails its partition, because the statements after it depend on it.
    /// The statements accepted before it stay in the catalogs, which cannot affect another partition: every name
    /// carries its partition's key.
    /// An unbindable test case yields an error entry, not a failed partition: a test may expect that error.
    SystestBinder(const SystestConfiguration& config, std::span<const PartitionToBind> partitions);

    /// One result per partition, in the order they were given.
    [[nodiscard]] const std::vector<BindResult>& getBound() const&;
    [[nodiscard]] std::vector<BindResult> getBound() &&;

    ~SystestBinder();

private:
    struct Impl;
    UniquePtr<Impl> impl;
};
}
