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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <nautilus/val_ptr.hpp>
#include <CompilationContext.hpp>
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
std::atomic<int64_t> totalCompilationTimeNanoseconds{0};
std::atomic<uint64_t> compilationCount{0};
std::atomic<int64_t> compilationWallTimeNanoseconds{0};
std::atomic<uint64_t> activeCompilationCount{0};
std::atomic<int64_t> compilationWallIntervalStartNanoseconds{0};

int64_t getNanosecondsSinceEpoch(std::chrono::steady_clock::time_point timePoint)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timePoint.time_since_epoch()).count();
}

class CompilationWallTimeGuard
{
public:
    explicit CompilationWallTimeGuard(int64_t compilationStartNanoseconds)
        : compilationEndNanoseconds(0), hasCompilationEndNanoseconds(false)
    {
        const auto previousCompilationCount = activeCompilationCount.fetch_add(1);
        if (previousCompilationCount == 0)
        {
            compilationWallIntervalStartNanoseconds.store(compilationStartNanoseconds);
        }
    }

    void setCompilationEndNanoseconds(int64_t compilationEndNanoseconds)
    {
        this->compilationEndNanoseconds = compilationEndNanoseconds;
        hasCompilationEndNanoseconds = true;
    }

    ~CompilationWallTimeGuard()
    {
        const auto previousCompilationCount = activeCompilationCount.fetch_sub(1);
        if (previousCompilationCount != 1)
        {
            return;
        }

        const auto compilationIntervalStartNanoseconds = compilationWallIntervalStartNanoseconds.load();
        const auto compilationIntervalEndNanoseconds
            = hasCompilationEndNanoseconds ? compilationEndNanoseconds : getNanosecondsSinceEpoch(std::chrono::steady_clock::now());
        compilationWallTimeNanoseconds.fetch_add(compilationIntervalEndNanoseconds - compilationIntervalStartNanoseconds);
    }

private:
    int64_t compilationEndNanoseconds;
    bool hasCompilationEndNanoseconds;
};

void registerRuntimeInputFormatterHandlesForOperator(
    const PhysicalOperator& physicalOperator,
    PipelineExecutionContext& pipelineExecutionContext,
    std::unordered_set<uint64_t>& seenRuntimeInputFormatterSlots)
{
    if (const auto scanOperator = physicalOperator.tryGet<ScanPhysicalOperator>(); scanOperator)
    {
        if (auto* const runtimeInputFormatterHandle = scanOperator->getBufferRef()->getRuntimeInputFormatterHandle();
            runtimeInputFormatterHandle != nullptr)
        {
            const auto runtimeInputFormatterSlot = scanOperator->getRuntimeInputFormatterSlot();
            const auto [_, isNewSlot] = seenRuntimeInputFormatterSlots.insert(runtimeInputFormatterSlot);
            PRECONDITION(
                isNewSlot,
                "Duplicate runtime input formatter slot {} in pipeline {}",
                runtimeInputFormatterSlot,
                pipelineExecutionContext.getPipelineId().getRawValue());
            pipelineExecutionContext.setRuntimeInputFormatterHandle(runtimeInputFormatterSlot, runtimeInputFormatterHandle);
        }
    }

    if (const auto childOperator = physicalOperator.getChild(); childOperator)
    {
        registerRuntimeInputFormatterHandlesForOperator(*childOperator, pipelineExecutionContext, seenRuntimeInputFormatterSlots);
    }
}

void registerRuntimeInputFormatterHandles(const std::shared_ptr<Pipeline>& pipeline, PipelineExecutionContext& pipelineExecutionContext)
{
    std::unordered_set<uint64_t> seenRuntimeInputFormatterSlots;
    registerRuntimeInputFormatterHandlesForOperator(pipeline->getRootOperator(), pipelineExecutionContext, seenRuntimeInputFormatterSlots);
}
}

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : engine(std::move(options))
    , compiledPipelineFunction(nullptr)
    , operatorHandlers(std::move(operatorHandlers))
    , pipeline(std::move(pipeline))
{
}

void CompiledExecutablePipelineStage::resetCompilationMetrics()
{
    totalCompilationTimeNanoseconds.store(0);
    compilationCount.store(0);
    compilationWallTimeNanoseconds.store(0);
    activeCompilationCount.store(0);
    compilationWallIntervalStartNanoseconds.store(0);
}

std::chrono::nanoseconds CompiledExecutablePipelineStage::getTotalCompilationTime()
{
    return std::chrono::nanoseconds(totalCompilationTimeNanoseconds.load());
}

std::chrono::nanoseconds CompiledExecutablePipelineStage::getCompilationWallTime()
{
    return std::chrono::nanoseconds(compilationWallTimeNanoseconds.load());
}

uint64_t CompiledExecutablePipelineStage::getCompilationCount()
{
    return compilationCount.load();
}

void CompiledExecutablePipelineStage::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    /// we call the compiled pipeline function with an input buffer and the execution context
    registerRuntimeInputFormatterHandles(pipeline, pipelineExecutionContext);
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    compiledPipelineFunction(std::addressof(pipelineExecutionContext), std::addressof(inputTupleBuffer), std::addressof(arena));
}

nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const TupleBuffer*, const Arena*>
CompiledExecutablePipelineStage::compilePipeline() const
{
    CPPTRACE_TRY
    {
        /// We must capture the operatorPipeline by value to ensure it is not destroyed before the function is called
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
        return engine.registerFunction(compiledFunction);
    }
    CPPTRACE_CATCH(...)
    {
        throw wrapExternalException(fmt::format("Could not query compile pipeline: {}", *pipeline));
    }
    std::unreachable();
}

void CompiledExecutablePipelineStage::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    pipeline->getRootOperator().terminate(ctx);
}

std::ostream& CompiledExecutablePipelineStage::toString(std::ostream& os) const
{
    return os << "CompiledExecutablePipelineStage()";
}

void CompiledExecutablePipelineStage::start(PipelineExecutionContext& pipelineExecutionContext)
{
    registerRuntimeInputFormatterHandles(pipeline, pipelineExecutionContext);
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    ExecutionContext ctx(std::addressof(pipelineExecutionContext), std::addressof(arena));
    CompilationContext compilationCtx{engine};
    pipeline->getRootOperator().setup(ctx, compilationCtx);
    const auto compileStart = std::chrono::steady_clock::now();
    CompilationWallTimeGuard compilationWallTimeGuard(getNanosecondsSinceEpoch(compileStart));
    compiledPipelineFunction = this->compilePipeline();
    const auto compileEnd = std::chrono::steady_clock::now();
    compilationWallTimeGuard.setCompilationEndNanoseconds(getNanosecondsSinceEpoch(compileEnd));
    const auto compileDuration = std::chrono::duration_cast<std::chrono::nanoseconds>(compileEnd - compileStart);
    totalCompilationTimeNanoseconds.fetch_add(compileDuration.count());
    compilationCount.fetch_add(1);
}

}
