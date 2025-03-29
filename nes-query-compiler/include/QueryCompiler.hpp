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

#include <optional>
#include <unordered_map>
#include <Plans/QueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES::QueryCompilation
{

/// Represents a query compilation request.
struct QueryCompilationRequest
{
    std::unique_ptr<QueryPlan> queryPlan;

    /// IMPORTANT: only the queryPlan should influence the actual result, other request options only influence how much to debug print etc.
    bool debug = false;
    bool dumpQueryPlans = false;
};

/// The query compiler behaves as a pure function: QueryPlan -> ExecutableQueryPlan
/// This guarantees that identical QueryPlan instances produce identical ExecutableQueryPlan results.
class QueryCompiler
{
public:
    QueryCompiler(const std::shared_ptr<NodeEngine> nodeEngine);

    std::unique_ptr<ExecutableQueryPlan> compileQuery(std::unique_ptr<QueryCompilationRequest> request);

private:
    std::shared_ptr<NodeEngine> nodeEngine;
};

}
