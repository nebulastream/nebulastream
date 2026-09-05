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

#include <InferModelPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <InferenceRuntime.hpp>
#include <val_ptr.hpp>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <Model.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <static.hpp>
#include <val_arith.hpp>

namespace NES::detail
{

/// Per-operator pool of inference runtimes, one per worker thread. Owns the
/// compiled model (for deferred init at setup) and the thread-local runtime
/// vector.
struct ThreadLocalRuntimeWrapper
{
    explicit ThreadLocalRuntimeWrapper(CompiledModel model) : model(std::move(model)) { }

    void setup(size_t numThreads)
    {
        wrappers.clear();
        wrappers.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i)
        {
            wrappers.emplace_back();
            wrappers.back().setup(model);
        }
    }

    [[nodiscard]] InferenceRuntime& getHandle(WorkerThreadId thread) { return wrappers[thread.getRawValue() % wrappers.size()]; }

    CompiledModel model;
    std::vector<InferenceRuntime> wrappers;
};

}

namespace NES
{

namespace
{

using detail::ThreadLocalRuntimeWrapper;

ThreadLocalRuntimeWrapper* resolveRuntime(const RuntimeStateRegistry* runtimeStateRegistry, const uint64_t runtimeStateSlot)
{
    PRECONDITION(runtimeStateRegistry != nullptr, "Runtime state registry should not be null");
    auto* const state = runtimeStateRegistry->getState(runtimeStateSlot, RuntimeStateType::INFERENCE_RUNTIME);
    PRECONDITION(state != nullptr, "Missing inference runtime state for slot {}", runtimeStateSlot);
    return static_cast<ThreadLocalRuntimeWrapper*>(state);
}

void setupSessions(ThreadLocalRuntimeWrapper* twl, PipelineExecutionContext* pec)
{
    twl->setup(pec->getNumberOfWorkerThreads());
}

int8_t* getInputBuffer(ThreadLocalRuntimeWrapper* twl, WorkerThreadId thread)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) std::byte* to int8_t* for nautilus pointer arithmetic
    return reinterpret_cast<int8_t*>(twl->getHandle(thread).getInputData());
}

int8_t* getOutputBuffer(ThreadLocalRuntimeWrapper* twl, WorkerThreadId thread)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) std::byte* to int8_t* for nautilus pointer arithmetic
    return reinterpret_cast<int8_t*>(twl->getHandle(thread).getOutputData());
}

void infer(ThreadLocalRuntimeWrapper* twl, WorkerThreadId thread)
{
    twl->getHandle(thread).infer();
}

}

InferModelPhysicalOperator::InferModelPhysicalOperator(
    CompiledModel model,
    std::vector<QualifiedIdentifier> inputFieldNames,
    std::vector<QualifiedIdentifier> outputFieldNames,
    bool varsizedInput,
    bool varsizedOutput)
    : inputFieldNames(std::move(inputFieldNames))
    , outputFieldNames(std::move(outputFieldNames))
    , outputSize(model.outputSize())
    , varsizedInput(varsizedInput)
    , varsizedOutput(varsizedOutput)
{
    threadLocal = std::make_shared<ThreadLocalRuntimeWrapper>(std::move(model));
}

void InferModelPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    runtimeStateSlot = compilationContext.registerRuntimeState(RuntimeStateType::INFERENCE_RUNTIME, static_cast<void*>(threadLocal.get()));
    const auto runtime = nautilus::invoke(resolveRuntime, executionCtx.runtimeStateRegistry, nautilus::val<uint64_t>{runtimeStateSlot});
    nautilus::invoke(setupSessions, runtime, executionCtx.pipelineContext);
}

void InferModelPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto runtime = nautilus::invoke(resolveRuntime, ctx.runtimeStateRegistry, nautilus::val<uint64_t>{runtimeStateSlot});
    const auto inputBuffer = nautilus::invoke(getInputBuffer, runtime, ctx.workerThreadId);

    if (varsizedInput)
    {
        const auto& value = record.read(inputFieldNames.at(0));
        auto varSized = value.getRawValueAs<VariableSizedData>();
        nautilus::memcpy(inputBuffer, varSized.getContent(), varSized.getSize());
    }
    else
    {
        for (nautilus::static_val<size_t> i = 0; i < inputFieldNames.size(); ++i)
        {
            const auto value = record.read(inputFieldNames.at(nautilus::static_val<int>(i)));
            const auto memPos = inputBuffer + nautilus::val<uint64_t>(i * sizeof(float));
            value.writeToMemory(memPos);
        }
    }

    nautilus::invoke(infer, runtime, ctx.workerThreadId);

    const auto outputBuffer = nautilus::invoke(getOutputBuffer, runtime, ctx.workerThreadId);

    if (varsizedOutput)
    {
        const auto outputSizeBytes = nautilus::val<uint64_t>(outputSize);
        auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(outputSizeBytes);
        nautilus::memcpy(output.getContent(), outputBuffer, outputSizeBytes);
        record.write(outputFieldNames.at(0), VarVal(output));
    }
    else
    {
        const DataType floatType{DataType::Type::FLOAT32, DataType::NULLABLE::NOT_NULLABLE};
        for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
        {
            const auto memPos = outputBuffer + nautilus::val<uint64_t>(i * sizeof(float));
            const auto result = VarVal::readNonNullableVarValFromMemory(memPos, floatType);
            record.write(outputFieldNames.at(i), result);
        }
    }

    executeChild(ctx, record);
}

void InferModelPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> InferModelPhysicalOperator::getChild() const
{
    return child;
}

void InferModelPhysicalOperator::setChild(PhysicalOperator newChild)
{
    this->child = std::move(newChild);
}

}
