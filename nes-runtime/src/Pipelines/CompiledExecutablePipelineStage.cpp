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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
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
std::atomic<uint64_t> nextDynamicPointerBindingFileId{0};
constexpr std::string_view kDynamicPointerBindingVersion = "nautilus.mlirbc.dynptr.bindings.v1";

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

using DynamicPointerBindings = std::vector<RuntimeDynamicPointerBinding>;

void collectDynamicPointerBindingsForOperator(
    const PhysicalOperator& physicalOperator,
    DynamicPointerBindings& dynamicPointerBindings,
    std::unordered_set<uint64_t>& seenRuntimeInputFormatterSlots,
    const PipelineId pipelineId)
{
    if (const auto scanOperator = physicalOperator.tryGet<ScanPhysicalOperator>(); scanOperator)
    {
        if (const auto runtimeInputFormatterBinding = scanOperator->getRuntimeInputFormatterBinding(); runtimeInputFormatterBinding)
        {
            const auto runtimeInputFormatterSlot = scanOperator->getRuntimeInputFormatterSlot();
            const auto [_, isNewSlot] = seenRuntimeInputFormatterSlots.insert(runtimeInputFormatterSlot);
            PRECONDITION(isNewSlot, "Duplicate runtime input formatter slot {} in pipeline {}", runtimeInputFormatterSlot, pipelineId.getRawValue());
            dynamicPointerBindings.emplace_back(*runtimeInputFormatterBinding);
        }
    }

    if (const auto childOperator = physicalOperator.getChild(); childOperator)
    {
        collectDynamicPointerBindingsForOperator(*childOperator, dynamicPointerBindings, seenRuntimeInputFormatterSlots, pipelineId);
    }
}

DynamicPointerBindings collectDynamicPointerBindings(const std::shared_ptr<Pipeline>& pipeline)
{
    DynamicPointerBindings dynamicPointerBindings;
    std::unordered_set<uint64_t> seenRuntimeInputFormatterSlots;
    collectDynamicPointerBindingsForOperator(
        pipeline->getRootOperator(), dynamicPointerBindings, seenRuntimeInputFormatterSlots, pipeline->getPipelineId());
    return dynamicPointerBindings;
}

std::filesystem::path createDynamicPointerBindingPath(const PipelineId pipelineId)
{
    auto bindingPath = std::filesystem::temp_directory_path();
    bindingPath /= fmt::format(
        "nes-nautilus-dynptr-{}-{}-{}.bindings",
        pipelineId.getRawValue(),
        getNanosecondsSinceEpoch(std::chrono::steady_clock::now()),
        nextDynamicPointerBindingFileId.fetch_add(1));
    return bindingPath;
}

std::filesystem::path writeDynamicPointerBindings(const DynamicPointerBindings& dynamicPointerBindings, const PipelineId pipelineId)
{
    const auto bindingPath = createDynamicPointerBindingPath(pipelineId);
    std::filesystem::create_directories(bindingPath.parent_path());

    std::ofstream output(bindingPath);
    PRECONDITION(output.is_open(), "Could not open dynamic pointer binding file {}", bindingPath.string());
    output << kDynamicPointerBindingVersion << "\n";

    auto sortedBindings = dynamicPointerBindings;
    std::ranges::sort(sortedBindings, {}, [](const auto& binding) { return binding.getName(); });
    for (const auto& binding : sortedBindings)
    {
        output << binding.getName() << "\t" << fmt::format("{:#x}", binding.getPointerValue()) << "\n";
    }
    PRECONDITION(output.good(), "Could not write dynamic pointer binding file {}", bindingPath.string());
    return bindingPath;
}

nautilus::engine::Options
configureOptionsForDynamicPointerBindings(const DynamicPointerBindings& dynamicPointerBindings, const PipelineId pipelineId, nautilus::engine::Options options)
{
    if (dynamicPointerBindings.empty())
    {
        return options;
    }

    const auto bindingPath = writeDynamicPointerBindings(dynamicPointerBindings, pipelineId);
    options.setOption("engine.Blob.DynamicPointerBindingPath", bindingPath.string());
    return options;
}
}

CompiledExecutablePipelineStage::CompiledExecutablePipelineStage(
    std::shared_ptr<Pipeline> pipeline,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers,
    nautilus::engine::Options options)
    : dynamicPointerBindings(collectDynamicPointerBindings(pipeline))
    , engine(configureOptionsForDynamicPointerBindings(dynamicPointerBindings, pipeline->getPipelineId(), std::move(options)))
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
    pipelineExecutionContext.setOperatorHandlers(operatorHandlers);
    Arena arena(pipelineExecutionContext.getBufferManager());
    compiledPipelineFunction(std::addressof(pipelineExecutionContext), std::addressof(inputTupleBuffer), std::addressof(arena));
}

nautilus::engine::CallableFunction<void, PipelineExecutionContext*, const TupleBuffer*, const Arena*>
CompiledExecutablePipelineStage::compilePipeline() const
{
    CPPTRACE_TRY
    {
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
