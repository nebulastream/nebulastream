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

#include <SemanticMapPhysicalOperator.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <semaphore>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Record.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <SemanticBackend.hpp>
#include <SemanticLatencyStats.hpp>
#include <SemanticMapCodec.hpp>
#include <SemanticModelCatalog.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES::detail
{

/// Everything one worker thread touches while a record is in flight. The traced code takes
/// pointers into `answers`, which therefore stay untouched until the thread's next record.
struct SemanticMapSlot
{
    std::unique_ptr<SemanticBackend> backend;
    std::vector<RowPayload> rows;
    std::vector<std::string> answers;
};

struct SemanticMapState
{
    /// Stage 1 has no configurable connect timeout; ten seconds fails a dead host quickly while
    /// staying far above any LAN or loopback handshake.
    static constexpr std::chrono::milliseconds ConnectTimeout{10000};

    SemanticMapState(SemanticBackendProvider backendProvider, const SemanticModelConfig& config, std::vector<std::string> inputNames)
        : backendProvider(std::move(backendProvider))
        , codec(config)
        , request{
              .prompt = {},
              .modelName = config.modelName,
              .timeout = config.requestTimeout,
              .connectTimeout = ConnectTimeout,
              .maxRetries = config.maxRetries}
        , inputNames(std::move(inputNames))
        , numberOfSteps(config.steps.size())
        , inFlight(static_cast<std::ptrdiff_t>(config.maxConcurrency))
    {
        PRECONDITION(config.maxConcurrency >= 1, "SemanticMap requires a max concurrency of at least 1");
    }

    void setup(const size_t numberOfWorkerThreads)
    {
        slots.clear();
        slots.reserve(numberOfWorkerThreads);
        for (size_t i = 0; i < numberOfWorkerThreads; ++i)
        {
            auto& slot = slots.emplace_back(SemanticMapSlot{.backend = backendProvider(), .rows = {}, .answers = {}});
            /// Stage 1 sends one record per request, under the same id the sysprompt's example uses.
            auto& row = slot.rows.emplace_back(RowPayload{.rowId = "row1", .fields = {}});
            for (const auto& name : inputNames)
            {
                row.fields.emplace_back(name, std::string{});
            }
            slot.answers.resize(numberOfSteps);
        }
    }

    [[nodiscard]] SemanticMapSlot& getSlot(const WorkerThreadId thread)
    {
        /// Direct indexing on purpose: a modulo would let two threads share one non-thread-safe
        /// backend and one scratch buffer if a thread id ever exceeded the configured worker count.
        const auto index = thread.getRawValue();
        INVARIANT(index < slots.size(), "WorkerThreadId {} is out of range for {} semantic map slots", index, slots.size());
        return slots[index];
    }

    void process(const WorkerThreadId thread)
    {
        auto& slot = getSlot(thread);
        auto completion = request;
        completion.prompt = codec.buildPrompt(slot.rows);

        std::expected<std::string, BackendError> response;
        {
            /// MAX_CONCURRENCY bounds this operator's requests in flight across all of its worker
            /// threads. As every thread blocks on its own request, it only binds below the thread count.
            inFlight.acquire();
            const Releaser releaser{inFlight};
            response = slot.backend->complete(completion);
        }

        if (!response.has_value() && response.error().kind != BackendError::Kind::MALFORMED_RESPONSE)
        {
            throw InferenceRuntimeFailure("Semantic model '{}': {}", completion.modelName, response.error().message);
        }
        /// A 2xx body that is not a chat completion counts as an unusable answer, not as a failed
        /// transport: every step falls back to its default value.
        const auto answers = codec.parse(response.value_or(std::string{}), slot.rows);
        slot.answers = answers.front();
    }

    /// Releases the semaphore on every exit path, including a throwing backend.
    struct Releaser
    {
        std::counting_semaphore<>& semaphore;

        explicit Releaser(std::counting_semaphore<>& semaphore) : semaphore(semaphore) { }

        Releaser(const Releaser&) = delete;
        Releaser& operator=(const Releaser&) = delete;
        Releaser(Releaser&&) = delete;
        Releaser& operator=(Releaser&&) = delete;

        ~Releaser() { semaphore.release(); }
    };

    SemanticBackendProvider backendProvider;
    SemanticMapCodec codec;
    CompletionRequest request;
    std::vector<std::string> inputNames;
    size_t numberOfSteps;
    std::counting_semaphore<> inFlight;
    std::vector<SemanticMapSlot> slots;
};

}

namespace NES
{

namespace
{

using detail::SemanticMapState;

void setupSemanticMap(SemanticMapState* state, PipelineExecutionContext* pec)
{
    state->setup(pec->getNumberOfWorkerThreads());
}

void terminateSemanticMap(SemanticMapState* state)
{
    /// Closes the endpoint connections when the query stops rather than when the plan is destroyed.
    state->slots.clear();
    /// One latency summary per query, from the same recorder the asynchronous executor reports
    /// through, so the two modes are comparable line for line.
    SemanticLatencyStats::instance().logAndReset("synchronous");
}

void setInput(SemanticMapState* state, const WorkerThreadId thread, const uint64_t fieldIndex, const int8_t* content, const uint64_t size)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) VARSIZED bytes are the field's text
    state->getSlot(thread).rows.front().fields[fieldIndex].second.assign(reinterpret_cast<const char*>(content), size);
}

void processRecord(SemanticMapState* state, const WorkerThreadId thread)
{
    state->process(thread);
}

const int8_t* getAnswer(SemanticMapState* state, const WorkerThreadId thread, const uint64_t step)
{
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) char* to int8_t* for nautilus::memcpy
    return reinterpret_cast<const int8_t*>(state->getSlot(thread).answers[step].data());
}

uint64_t getAnswerSize(SemanticMapState* state, const WorkerThreadId thread, const uint64_t step)
{
    return state->getSlot(thread).answers[step].size();
}

}

SemanticMapPhysicalOperator::SemanticMapPhysicalOperator(
    SemanticBackendProvider backendProvider,
    SemanticModelConfig config,
    std::vector<QualifiedIdentifier> inputFields,
    std::vector<QualifiedIdentifier> outputFields)
    : inputFields(std::move(inputFields)), outputFields(std::move(outputFields))
{
    PRECONDITION(
        this->outputFields.size() == config.steps.size(),
        "SemanticMap writes one output field per step, but got {} fields for {} steps",
        this->outputFields.size(),
        config.steps.size());
    std::vector<std::string> inputNames;
    inputNames.reserve(this->inputFields.size());
    for (const auto& field : this->inputFields)
    {
        inputNames.push_back(fmt::format("{}", field));
    }
    state = std::make_shared<SemanticMapState>(std::move(backendProvider), config, std::move(inputNames));
}

void SemanticMapPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    setupChild(executionCtx, compilationContext);
    nautilus::invoke(setupSemanticMap, nautilus::val<SemanticMapState*>(state.get()), executionCtx.pipelineContext);
}

void SemanticMapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto statePtr = nautilus::val<SemanticMapState*>(state.get());

    /// static_val unrolls at trace time, so the host-side vector lookups are legal in the traced body.
    for (nautilus::static_val<size_t> i = 0; i < inputFields.size(); ++i)
    {
        const auto value = record.read(inputFields.at(i)).getRawValueAs<VariableSizedData>();
        nautilus::invoke(setInput, statePtr, ctx.workerThreadId, nautilus::val<uint64_t>(i), value.getContent(), value.getSize());
    }

    nautilus::invoke(processRecord, statePtr, ctx.workerThreadId);

    /// The answer's length is known once the round trip is done, so each field is allocated at its
    /// exact size rather than at an estimated upper bound.
    for (nautilus::static_val<size_t> i = 0; i < outputFields.size(); ++i)
    {
        const auto step = nautilus::val<uint64_t>(i);
        const auto answer = nautilus::invoke(getAnswer, statePtr, ctx.workerThreadId, step);
        const auto size = nautilus::invoke(getAnswerSize, statePtr, ctx.workerThreadId, step);
        auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(size);
        nautilus::memcpy(output.getContent(), answer, size);
        record.write(outputFields.at(i), VarVal(output));
    }

    executeChild(ctx, record);
}

void SemanticMapPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(terminateSemanticMap, nautilus::val<SemanticMapState*>(state.get()));
    terminateChild(executionCtx);
}

std::optional<PhysicalOperator> SemanticMapPhysicalOperator::getChild() const
{
    return child;
}

void SemanticMapPhysicalOperator::setChild(PhysicalOperator newChild)
{
    child = std::move(newChild);
}

}
