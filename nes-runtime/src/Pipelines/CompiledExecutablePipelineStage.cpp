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
#include <Pipelines/CompiledExecutablePipelineStage.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/from_current.hpp>
#include <cpptrace/from_current_macros.hpp>
#include <fmt/format.h>
#include <nautilus/val_ptr.hpp>
#include <CompilationContext.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <function.hpp>
#include <options.hpp>

namespace NES
{

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : engine(options)
    , operatorHandlers(std::move(operatorHandlers))
    , rootOperator(pipeline->getRootOperator())
    , pipelineId(pipeline->getPipelineId())
{
}

void CompiledExecutablePipelineStage::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    INVARIANT(compiledPipelineFunction.has_value(), "execute() was called before start() compiled the pipeline");
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    (*compiledPipelineFunction)(std::addressof(pipelineExecutionContext), std::addressof(inputTupleBuffer), std::addressof(arena));
}

void CompiledExecutablePipelineStage::registerPipelineFunction(nautilus::engine::NautilusModule& module) const
{
    /// Capture the stage by pointer: this compiled function is only ever invoked through execute()/start()/stop() on
    /// the owning stage, so the stage (and thus its root operator) outlives every call. Capturing the root operator by
    /// value instead makes the compiled module co-own its state, and cached slices can keep that module alive past
    /// teardown, which leaks buffers in the sliceCache systests.
    /// Additionally, we can NOT use const or const references for the parameters of the lambda function
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    const std::function<void(nautilus::val<PipelineExecutionContext*>, nautilus::val<const TupleBuffer*>, nautilus::val<const Arena*>)>
        compiledFunction = [this](
                               nautilus::val<PipelineExecutionContext*> pipelineExecutionContext,
                               nautilus::val<const TupleBuffer*> recordBufferRef,
                               nautilus::val<const Arena*> arenaRef)
    {
        auto ctx = ExecutionContext(pipelineExecutionContext, arenaRef);
        RecordBuffer recordBuffer(recordBufferRef);

        rootOperator.open(ctx, recordBuffer);
        switch (ctx.getOpenReturnState())
        {
            case OpenReturnState::CONTINUE: {
                rootOperator.close(ctx, recordBuffer);
                break;
            }
            case OpenReturnState::REPEAT: {
                nautilus::invoke(
                    +[](PipelineExecutionContext* pec, const TupleBuffer* buffer)
                    { pec->repeatTask(*buffer, std::chrono::milliseconds(0)); },
                    pipelineExecutionContext,
                    recordBufferRef);
                break;
            }
        }
    };
    /// NOLINTEND(performance-unnecessary-value-param)
    module.registerFunction(std::string{PIPELINE_FUNCTION_NAME}, compiledFunction);
}

void CompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    rootOperator.terminate(ctx);
}

std::ostream& CompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    return os << "CompiledExecutablePipelineStage()";
}

void CompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    /// Each pipeline compiles into exactly one module: operators register named helper functions during setup(),
    /// the main pipeline function is added to the same module, and a single compile() call traces and compiles
    /// all of them together. Only afterwards do the handles handed out during setup() become invocable.
    CPPTRACE_TRY
    {
        auto module = engine.createModule();
        CompilationContext compilationCtx{module};
        rootOperator.setup(ctx, compilationCtx);
        registerPipelineFunction(module);
        compiledModule = module.compile();
        compilationCtx.resolveAfterCompilation(*compiledModule);
        compiledPipelineFunction = compiledModule->getFunction<PipelineSignature>(std::string{PIPELINE_FUNCTION_NAME});

        /// Surface nautilus' per-compilation statistics (tracing/IR/backend timings, generated code size).
        /// getStatistics() is null in interpreted mode; the report is only formatted when debug logging is on.
        if (const auto statistics = compiledModule->getStatistics())
        {
            NES_DEBUG(
                "Nautilus compilation statistics for pipeline {}:\n{}",
                pipelineId,
                statistics->formatReport(fmt::format("pipeline-{}", pipelineId), engine.getNameOfBackend()));
        }
    }
    CPPTRACE_CATCH(...)
    {
        throw wrapExternalException(fmt::format("Could not query compile pipeline {}", pipelineId));
    }
}

}
