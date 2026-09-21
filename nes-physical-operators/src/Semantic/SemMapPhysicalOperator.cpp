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

#include <SemMapPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <val_ptr.hpp>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <LlmClient.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <static.hpp>
#include <val_arith.hpp>

namespace NES::detail
{

struct PerThreadSlot
{
    std::unique_ptr<LlmClient> client;
    /// Answers for the record currently in flight, in declared-OUTPUT order. Stable for the
    /// duration of one execute(): the traced code takes pointers into these strings.
    std::vector<std::string> answers;
    /// Space-joined INPUT field values for the record currently in flight (plan §M1, LlmClient.hpp
    /// contract). Same lifetime guarantee as `answers`.
    std::string inputScratch;
};

/// Per-operator pool of LLM clients, one per worker thread — a `CURL*`-backed client is
/// non-copyable/non-movable, so a single instance cannot serve N worker threads.
struct ThreadLocalLlmClients
{
    ThreadLocalLlmClients(LlmClientFactory factory, std::vector<std::string> outputFieldNames)
        : factory(std::move(factory)), outputFieldNames(std::move(outputFieldNames))
    {
    }

    void setup(size_t numThreads)
    {
        slots.clear();
        slots.reserve(numThreads);
        for (size_t i = 0; i < numThreads; ++i)
        {
            slots.emplace_back(PerThreadSlot{
                .client = factory(), .answers = std::vector<std::string>(outputFieldNames.size()), .inputScratch = {}});
        }
    }

    [[nodiscard]] PerThreadSlot& getSlot(WorkerThreadId thread)
    {
        /// Indexing directly (not `% slots.size()`) is deliberate: a modulo would silently let two
        /// threads share one non-thread-safe CURL* and one scratch buffer if a WorkerThreadId ever
        /// exceeded the configured worker count — data corruption, not a clean failure.
        const auto index = thread.getRawValue();
        INVARIANT(index < slots.size(), "WorkerThreadId {} is out of range for {} thread-local slots", index, slots.size());
        return slots[index];
    }

    LlmClientFactory factory;
    std::vector<std::string> outputFieldNames;
    std::vector<PerThreadSlot> slots;
};

}

namespace NES
{

namespace
{

using detail::ThreadLocalLlmClients;

void setupClients(ThreadLocalLlmClients* tl, PipelineExecutionContext* pec)
{
    tl->setup(pec->getNumberOfWorkerThreads());
}

void resetInput(ThreadLocalLlmClients* tl, WorkerThreadId thread)
{
    tl->getSlot(thread).inputScratch.clear();
}

void appendInput(ThreadLocalLlmClients* tl, const int8_t* content, uint64_t size, WorkerThreadId thread)
{
    auto& slot = tl->getSlot(thread);
    if (!slot.inputScratch.empty())
    {
        slot.inputScratch.push_back(' ');
    }
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) int8_t* to char* for std::string_view construction
    slot.inputScratch.append(reinterpret_cast<const char*>(content), static_cast<size_t>(size));
}

/// One HTTP round-trip. Blocks this worker thread for the full LLM latency — accepted for
/// Phase 1 (plan §M1 "Open — blocking I/O inside a pipeline task").
void semMapCall(ThreadLocalLlmClients* tl, WorkerThreadId thread)
{
    auto& slot = tl->getSlot(thread);
    const auto result = slot.client->map(slot.inputScratch);
    for (size_t i = 0; i < tl->outputFieldNames.size(); ++i)
    {
        if (const auto it = result.find(tl->outputFieldNames[i]); it != result.end())
        {
            slot.answers[i] = it->second.answer;
        }
        else
        {
            slot.answers[i].clear();
        }
    }
}

const int8_t* getAnswerPtr(ThreadLocalLlmClients* tl, WorkerThreadId thread, uint64_t fieldIdx)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char* to int8_t* for nautilus pointer arithmetic
    return reinterpret_cast<const int8_t*>(tl->getSlot(thread).answers[fieldIdx].data());
}

uint64_t getAnswerSize(ThreadLocalLlmClients* tl, WorkerThreadId thread, uint64_t fieldIdx)
{
    return tl->getSlot(thread).answers[fieldIdx].size();
}

}

SemMapPhysicalOperator::SemMapPhysicalOperator(
    LlmClientFactory clientFactory,
    std::vector<QualifiedIdentifier> inputFieldNames,
    std::vector<QualifiedIdentifier> outputFieldNames,
    std::vector<std::string> modelOutputNames)
    : inputFieldNames(std::move(inputFieldNames)), outputFieldNames(std::move(outputFieldNames))
{
    PRECONDITION(
        this->outputFieldNames.size() == modelOutputNames.size(),
        "SemMap output field names ({}) and model output names ({}) must be parallel",
        this->outputFieldNames.size(),
        modelOutputNames.size());
    threadLocal
        = std::make_shared<detail::ThreadLocalLlmClients>(std::move(clientFactory), std::move(modelOutputNames));
}

void SemMapPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    nautilus::invoke(
        setupClients, nautilus::val<detail::ThreadLocalLlmClients*>(threadLocal.get()), executionCtx.pipelineContext);
}

void SemMapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto state = nautilus::val<detail::ThreadLocalLlmClients*>(threadLocal.get());

    nautilus::invoke(resetInput, state, ctx.workerThreadId);
    /// static_val unrolls at trace time, so the host-side .at(i) lookup is legal inside the
    /// traced body. Single input stays byte-identical (no separator): appendInput only inserts a
    /// space once the scratch buffer is non-empty.
    for (nautilus::static_val<size_t> i = 0; i < inputFieldNames.size(); ++i)
    {
        const auto& value = record.read(inputFieldNames.at(i));
        auto varSized = value.getRawValueAs<VariableSizedData>();
        nautilus::invoke(appendInput, state, varSized.getContent(), varSized.getSize(), ctx.workerThreadId);
    }
    nautilus::invoke(semMapCall, state, ctx.workerThreadId);

    /// static_val unrolls at trace time, so the host-side .at(i) lookup is legal inside the
    /// traced body. Phase 1 exercises only the VARSIZED branch; the loop shape is what lets
    /// multi-output / typed models drop in later.
    for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
    {
        const auto idx = nautilus::val<uint64_t>(i);
        const auto ptr = nautilus::invoke(getAnswerPtr, state, ctx.workerThreadId, idx);
        const auto len = nautilus::invoke(getAnswerSize, state, ctx.workerThreadId, idx);
        auto out = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(len);
        nautilus::memcpy(out.getContent(), ptr, len);
        record.write(outputFieldNames.at(i), VarVal(out));
    }

    executeChild(ctx, record);
}

void SemMapPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> SemMapPhysicalOperator::getChild() const
{
    return child;
}

void SemMapPhysicalOperator::setChild(PhysicalOperator newChild)
{
    this->child = std::move(newChild);
}

}
