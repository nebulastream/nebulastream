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
#include <Plans/DecomposedQueryPlan.hpp>
#include <Plans/PhysicalQueryPlan.hpp>
#include <QueryCompilationRequest.hpp>
#include <QueryCompilerConfiguration.hpp>

namespace NES::QueryCompilation
{

struct QueryCompilationIntermediate
{
    /// Logical plan before logical optimizations
    std::optional<DecomposedQueryPlan> logicalPlan;

    /// Logical plan after logical optimizations
    std::optional<DecomposedQueryPlan> OptimizedLogicalPlan;

    /// Physical before physical optimization
    ///std::optional<PhysicalQueryPlan> physicalPlan;

    /// Pipelines after physical optimization
    // std::optional<std::vector<Pipelines>> pipelinedPhysicalPlan;

    /// Print all available query representations of the query compilation process
    /// std::string getQueryCompilationExplain();
};

class QueryCompiler
{
public:
    QueryCompiler(std::shared_ptr<QueryCompilerConfiguration> options);
    std::unique_ptr<ExecutableQueryPlan> compileQuery(std::shared_ptr<QueryCompilationRequest> request, QueryId queryId);

    /// Prints all logical optimization steps that will be applied during compilation
    // std::string getRegisteredLogicalOptimizations();

    /// Prints all physical optimization steps that will be applied during compilation
    // std::string getRegisteredPhysicalOptimizations();

private:
    // bool runLogicalOptimization(QueryId queryId);

    // bool lowerToPhysicalPlan(QueryId queryId);

    // bool runPhysicalOptimization(QueryId queryId);

    // bool pipelining(QueryId queryId);

    /// Stores all the queryCompilation intermediate representations. Used in case of adaptive recompilations.
    /// We assume here that the queryId is unique throughout the worker lifetime.
    std::unordered_map<QueryId, QueryCompilationIntermediate> queryCompilationState;

    std::shared_ptr<QueryCompilerConfiguration> options;
};

}
