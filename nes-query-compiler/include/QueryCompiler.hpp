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

#include <memory>
#include <optional>
#include <utility>

#include <Plans/LogicalPlan.hpp>
#include <Util/DumpMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <QueryExecutionConfiguration.hpp>

namespace NES::QueryCompilation
{

/// Represents a query compilation request.
struct QueryCompilationRequest
{
    LogicalPlan queryPlan;
    /// The optimized plan whose running state this plan will absorb. The lowering
    /// pass pairs corresponding logical operators before pipelines are formed.
    std::optional<LogicalPlan> donorQueryPlan;

    /// Only the query and optional donor plans influence the compiled result; other request options control diagnostics.
    bool debug = false;
    DumpMode dumpCompilationResult = DumpMode{DumpMode::Options::NONE, false};
};

/// The query compiler behaves as a pure function of the query plan and optional donor plan.
class QueryCompiler
{
public:
    explicit QueryCompiler(QueryExecutionConfiguration defaultQueryExecution) : defaultQueryExecution(std::move(defaultQueryExecution)) { };

    std::unique_ptr<CompiledQueryPlan> compileQuery(std::unique_ptr<QueryCompilationRequest> request);

private:
    QueryExecutionConfiguration defaultQueryExecution;
};

}
