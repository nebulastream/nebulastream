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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/from_current.hpp>
#include <cpptrace/from_current_macros.hpp>
#include <fmt/format.h>
#include <nautilus/val_ptr.hpp>
#include <CompilationContext.hpp>
#include <EmitPhysicalOperator.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <ScanPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>

namespace NES
{
namespace
{
void registerRuntimeInputFormatterHandlesForOperator(
    const PhysicalOperator& physicalOperator,
    RuntimeInputFormatterRegistry& runtimeInputFormatterRegistry,
    bool& foundRuntimeInputFormatter,
    const PipelineId pipelineId)
{
    if (const auto scanOperator = physicalOperator.tryGet<ScanPhysicalOperator>(); scanOperator && scanOperator->hasRuntimeInputFormatter())
    {
        PRECONDITION(!foundRuntimeInputFormatter, "Multiple runtime input formatters in pipeline {}", pipelineId.getRawValue());
        foundRuntimeInputFormatter = true;
        runtimeInputFormatterRegistry.registerInputFormatterHandle(scanOperator->getRuntimeInputFormatterHandle());
        runtimeInputFormatterRegistry.registerIndexerMetaDataHandle(scanOperator->getRuntimeIndexerMetaDataHandle());
        runtimeInputFormatterRegistry.registerNullValuesHandle(scanOperator->getRuntimeNullValuesHandle());
    }

    if (const auto childOperator = physicalOperator.getChild(); childOperator)
    {
        registerRuntimeInputFormatterHandlesForOperator(
            *childOperator, runtimeInputFormatterRegistry, foundRuntimeInputFormatter, pipelineId);
    }
}

RuntimeInputFormatterRegistry createRuntimeInputFormatterRegistry(const std::shared_ptr<Pipeline>& pipeline)
{
    RuntimeInputFormatterRegistry runtimeInputFormatterRegistry;
    bool foundRuntimeInputFormatter = false;
    registerRuntimeInputFormatterHandlesForOperator(
        pipeline->getRootOperator(), runtimeInputFormatterRegistry, foundRuntimeInputFormatter, pipeline->getPipelineId());
    return runtimeInputFormatterRegistry;
}

void registerRuntimeOutputFormatterHandlesForOperator(
    const PhysicalOperator& physicalOperator,
    RuntimeOutputFormatterRegistry& runtimeOutputFormatterRegistry,
    bool& foundRuntimeOutputFormatter,
    const PipelineId pipelineId)
{
    if (const auto emitOperator = physicalOperator.tryGet<EmitPhysicalOperator>();
        emitOperator && emitOperator->hasRuntimeOutputFormatter())
    {
        PRECONDITION(!foundRuntimeOutputFormatter, "Multiple runtime output formatters in pipeline {}", pipelineId.getRawValue());
        foundRuntimeOutputFormatter = true;
        runtimeOutputFormatterRegistry.registerFieldDelimiterHandle(emitOperator->getRuntimeFieldDelimiterHandle());
        runtimeOutputFormatterRegistry.registerTupleDelimiterHandle(emitOperator->getRuntimeTupleDelimiterHandle());
        for (uint64_t fieldIndex = 0; fieldIndex < emitOperator->getRuntimeOutputFormatterFieldCount(); ++fieldIndex)
        {
            runtimeOutputFormatterRegistry.registerFieldNameHandle(fieldIndex, emitOperator->getRuntimeFieldNameHandle(fieldIndex));
        }
    }

    if (const auto childOperator = physicalOperator.getChild(); childOperator)
    {
        registerRuntimeOutputFormatterHandlesForOperator(
            *childOperator, runtimeOutputFormatterRegistry, foundRuntimeOutputFormatter, pipelineId);
    }
}

RuntimeOutputFormatterRegistry createRuntimeOutputFormatterRegistry(const std::shared_ptr<Pipeline>& pipeline)
{
    RuntimeOutputFormatterRegistry runtimeOutputFormatterRegistry;
    bool foundRuntimeOutputFormatter = false;
    registerRuntimeOutputFormatterHandlesForOperator(
        pipeline->getRootOperator(), runtimeOutputFormatterRegistry, foundRuntimeOutputFormatter, pipeline->getPipelineId());
    return runtimeOutputFormatterRegistry;
}
}

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : engine(options), pipeline(std::move(pipeline))
{
    std::vector<std::pair<OperatorHandlerId, std::shared_ptr<OperatorHandler>>> sortedHandlers;
    sortedHandlers.reserve(operatorHandlers.size());
    std::ranges::move(operatorHandlers, std::back_inserter(sortedHandlers));
    std::ranges::sort(sortedHandlers, {}, [](const auto& handler) { return handler.first.getRawValue(); });

    auto nextSlot = INITIAL_OPERATOR_HANDLER_ID.getRawValue();
    for (auto& [handlerId, handler] : sortedHandlers)
    {
        const auto slot = OperatorHandlerId{nextSlot++};
        PRECONDITION(handler != nullptr, "Operator handler {} is null", handlerId.getRawValue());
        operatorHandlerSlots.emplace(handlerId, slot);
        this->operatorHandlers.emplace(slot, std::move(handler));
    }
}

void CompiledExecutablePipelineStage::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    INVARIANT(compiledPipelineFunction.has_value(), "execute() was called before start() compiled the pipeline");
    auto runtimeInputFormatterRegistry = createRuntimeInputFormatterRegistry(pipeline);
    auto runtimeOutputFormatterRegistry = createRuntimeOutputFormatterRegistry(pipeline);
    /// we call the compiled pipeline function with an input buffer and the execution context
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    (*compiledPipelineFunction)(
        std::addressof(pipelineExecutionContext),
        std::addressof(runtimeInputFormatterRegistry),
        std::addressof(runtimeOutputFormatterRegistry),
        std::addressof(runtimeStateRegistry),
        std::addressof(inputTupleBuffer),
        std::addressof(arena));
}

void CompiledExecutablePipelineStage::registerPipelineFunction(nautilus::engine::NautilusModule& module) const
{
    /// Capture the stage by pointer rather than the pipeline shared_ptr: this compiled function is only ever invoked
    /// through execute()/start()/stop() on the owning stage, so the stage (and thus its pipeline) outlives every call.
    /// Capturing the pipeline shared_ptr by value instead makes the compiled module co-own the pipeline, and because
    /// cached slices keep that module alive through their cleanup handle, it retains the pipeline (and its slice
    /// buffers) past teardown -- which leaks buffers in the sliceCache systests.
    /// Additionally, we can NOT use const or const references for the parameters of the lambda function
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    const std::function<void(
        nautilus::val<PipelineExecutionContext*>,
        nautilus::val<const RuntimeInputFormatterRegistry*>,
        nautilus::val<const RuntimeOutputFormatterRegistry*>,
        nautilus::val<const RuntimeStateRegistry*>,
        nautilus::val<const TupleBuffer*>,
        nautilus::val<const Arena*>)>
        compiledFunction = [this](
                               nautilus::val<PipelineExecutionContext*> pipelineExecutionContext,
                               nautilus::val<const RuntimeInputFormatterRegistry*> runtimeInputFormatterRegistry,
                               nautilus::val<const RuntimeOutputFormatterRegistry*> runtimeOutputFormatterRegistry,
                               nautilus::val<const RuntimeStateRegistry*> runtimeStateRegistry,
                               nautilus::val<const TupleBuffer*> recordBufferRef,
                               nautilus::val<const Arena*> arenaRef)
    {
        auto ctx = ExecutionContext(
            pipelineExecutionContext,
            runtimeInputFormatterRegistry,
            runtimeOutputFormatterRegistry,
            runtimeStateRegistry,
            arenaRef,
            std::addressof(operatorHandlerSlots));
        RecordBuffer recordBuffer(recordBufferRef);

        pipeline->getRootOperator().open(ctx, recordBuffer);
        switch (ctx.getOpenReturnState())
        {
            case OpenReturnState::CONTINUE: {
                pipeline->getRootOperator().close(ctx, recordBuffer);
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
    auto runtimeInputFormatterRegistry = createRuntimeInputFormatterRegistry(pipeline);
    auto runtimeOutputFormatterRegistry = createRuntimeOutputFormatterRegistry(pipeline);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(
        std::addressof(pipelineExecutionContext),
        std::addressof(runtimeInputFormatterRegistry),
        std::addressof(runtimeOutputFormatterRegistry),
        std::addressof(runtimeStateRegistry),
        std::addressof(arena),
        std::addressof(operatorHandlerSlots));
    pipeline->getRootOperator().terminate(ctx);
}

std::ostream& CompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    return os << "CompiledExecutablePipelineStage()";
}

void CompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    runtimeStateRegistry.clear();
    auto runtimeInputFormatterRegistry = createRuntimeInputFormatterRegistry(pipeline);
    auto runtimeOutputFormatterRegistry = createRuntimeOutputFormatterRegistry(pipeline);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(
        std::addressof(pipelineExecutionContext),
        std::addressof(runtimeInputFormatterRegistry),
        std::addressof(runtimeOutputFormatterRegistry),
        std::addressof(runtimeStateRegistry),
        std::addressof(arena),
        std::addressof(operatorHandlerSlots));
    /// Each pipeline compiles into exactly one module: operators register named helper functions during setup(),
    /// the main pipeline function is added to the same module, and a single compile() call traces and compiles
    /// all of them together. Only afterwards do the handles handed out during setup() become invocable.
    CPPTRACE_TRY
    {
        auto module = engine.createModule();
        CompilationContext compilationCtx{module, pipelineExecutionContext, runtimeStateRegistry};
        pipeline->getRootOperator().setup(ctx, compilationCtx);
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
                pipeline->getPipelineId(),
                statistics->formatReport(fmt::format("pipeline-{}", pipeline->getPipelineId()), engine.getNameOfBackend()));
        }
    }
    CPPTRACE_CATCH(...)
    {
        throw wrapExternalException(fmt::format("Could not query compile pipeline: {}", *pipeline));
    }
}
}
