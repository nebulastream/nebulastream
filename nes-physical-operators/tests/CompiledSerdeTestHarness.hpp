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

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>
#include <CompilationContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <PipelineState.hpp>
#include <options.hpp>

namespace NES::Testing
{
/// Context and compilation driver for testing an operator's real compiled state codec.
/// Other operators can supply their own registration callback and compare their own state.
class SerdePipelineContext final : public PipelineExecutionContext
{
public:
    explicit SerdePipelineContext(const uint64_t workers)
        : buffers(BufferManager::create(8 * 1024 * 1024, 0.8, BufferAlignment(64), 1024, std::make_shared<NesDefaultMemoryAllocator>()))
        , workers(workers)
    {
    }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { throw std::runtime_error("Unexpected pipeline output"); }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { throw std::runtime_error("Unexpected repeat task"); }

    TupleBuffer allocateTupleBuffer() override { return buffers->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return workers; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return buffers; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& replacement) override
    {
        handlers = replacement;
    }

    std::shared_ptr<BufferManager> buffers;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    uint64_t workers;
};

class CompiledSerdeTestHarness
{
public:
    template <typename Register>
    static void compile(Register&& registerFunctions)
    {
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", true);
        options.setOption("engine.backend", std::string("mlir"));
        options.setOption("engine.compilationStrategy", std::string("legacy"));
        nautilus::engine::NautilusEngine engine(options);
        auto module = engine.createModule();
        CompilationContext context(module);
        std::invoke(std::forward<Register>(registerFunctions), context);
        auto compiled = module.compile();
        context.resolveAfterCompilation(compiled);
    }

    template <typename Operator>
    static TupleBuffer emit(const Operator& emitter, SerdePipelineContext& context)
    {
        PipelineStateBuilder state;
        emitter.emit(state, context);
        return state.finish(context.getBufferManager());
    }

    template <typename Operator>
    static void absorb(const Operator& absorber, const TupleBuffer& buffer, SerdePipelineContext& context)
    {
        PipelineStateReader state(buffer);
        absorber.absorb(state, context);
        state.ensureConsumed();
    }
};
}
