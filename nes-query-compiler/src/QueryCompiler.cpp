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


#include <QueryCompiler.hpp>

#include <memory>
#include <string>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <Phases/LowerToPhysicalOperators.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <Util/DumpMode.hpp>
#include <Util/ExecutionMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <nautilus/Engine.hpp>
#include <options.hpp>

namespace NES::QueryCompilation
{

QueryCompiler::QueryCompiler(QueryExecutionConfiguration defaultQueryExecution)
    : defaultQueryExecution(std::move(defaultQueryExecution))
{
    /// Build the single Nautilus engine once and reuse it for every pipeline compiled by this worker.
    /// These engine-scope options are fixed for the worker's lifetime: the execution mode comes from the worker
    /// configuration and is applied uniformly to all pipelines; only the per-request dump options vary, and those
    /// are module-scope and applied when each pipeline creates its module.
    nautilus::engine::Options options;
    /// We disable multithreading in MLIR by default to not interfere with NebulaStream's thread model
    options.setOption("mlir.enableMultithreading", false);
    /// Use the single-tier LegacyCompiler with the MLIR backend. Otherwise nautilus defaults to its tiered JIT
    /// (bytecode tier-0, MLIR tier-1 promoted on a background thread), whose bytecode backend is intentionally not
    /// built into our nautilus package and which would also conflict with the thread model above. We set both the
    /// strategy and the backend explicitly rather than relying on the "non-empty backend implies legacy" shortcut.
    options.setOption("engine.compilationStrategy", std::string("legacy"));
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.Compilation", this->defaultQueryExecution.executionMode.getValue() == ExecutionMode::COMPILER);
    engine = std::make_shared<const nautilus::engine::NautilusEngine>(options);
}

/// This phase should be as dumb as possible and not further decisions should be made here.
std::unique_ptr<CompiledQueryPlan> QueryCompiler::compileQuery(std::unique_ptr<QueryCompilationRequest> request)
{
    auto lowerToCompiledQueryPlanPhase = LowerToCompiledQueryPlanPhase(request->dumpCompilationResult, engine);
    auto queryPlan = LowerToPhysicalOperators::apply(request->queryPlan, defaultQueryExecution);
    auto pipelinedQueryPlan = PipeliningPhase::apply(queryPlan);
    return lowerToCompiledQueryPlanPhase.apply(pipelinedQueryPlan);
}
}
