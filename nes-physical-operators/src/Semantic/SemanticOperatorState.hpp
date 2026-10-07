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
#include <cstddef>
#include <expected>
#include <memory>
#include <semaphore>
#include <string>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES::detail
{

/// Everything one worker thread touches while a record is in flight. The traced code takes
/// pointers into `answers`, which therefore stay untouched until the thread's next record.
struct SemanticSlot
{
    std::unique_ptr<SemanticBackend> backend;
    std::vector<RowPayload> rows;
    /// One answer per MAP step, in step order.
    std::vector<std::string> answers;
    /// Whether the record passes; only a filtering operator ever clears it.
    bool passed = true;
};

/// The state the synchronous semantic operators share: one backend and one scratch row per worker
/// thread, the concurrency bound, and the round trip. `Codec` lays out the prompt; decoding the
/// response is the operator's own business, so it stays in the derived state.
template <typename Codec>
class SemanticOperatorState
{
public:
    /// Stage 1 has no configurable connect timeout; ten seconds fails a dead host quickly while
    /// staying far above any LAN or loopback handshake.
    static constexpr std::chrono::milliseconds ConnectTimeout{10000};

    SemanticOperatorState(
        SemanticBackendProvider backendProvider, const SemanticModelConfig& config, std::vector<std::string> inputNames, size_t numberOfAnswers)
        : codec(config)
        , backendProvider(std::move(backendProvider))
        , request{
              .prompt = {},
              .modelName = config.modelName,
              .timeout = config.requestTimeout,
              .connectTimeout = ConnectTimeout,
              .maxRetries = config.maxRetries}
        , inputNames(std::move(inputNames))
        , numberOfAnswers(numberOfAnswers)
        , inFlight(static_cast<std::ptrdiff_t>(config.maxConcurrency))
    {
        PRECONDITION(config.maxConcurrency >= 1, "A semantic operator requires a max concurrency of at least 1");
    }

    void setup(const size_t numberOfWorkerThreads)
    {
        slots.clear();
        slots.reserve(numberOfWorkerThreads);
        for (size_t i = 0; i < numberOfWorkerThreads; ++i)
        {
            auto& slot = slots.emplace_back(SemanticSlot{.backend = backendProvider(), .rows = {}, .answers = {}, .passed = true});
            /// Stage 1 sends one record per request, under the same id the sysprompt's example uses.
            auto& row = slot.rows.emplace_back(RowPayload{.rowId = "row1", .fields = {}});
            for (const auto& name : inputNames)
            {
                row.fields.emplace_back(name, std::string{});
            }
            slot.answers.resize(numberOfAnswers);
        }
    }

    /// Closes the endpoint connections when the query stops rather than when the plan is destroyed.
    void terminate() { slots.clear(); }

    [[nodiscard]] SemanticSlot& getSlot(const WorkerThreadId thread)
    {
        /// Direct indexing on purpose: a modulo would let two threads share one non-thread-safe
        /// backend and one scratch buffer if a thread id ever exceeded the configured worker count.
        const auto index = thread.getRawValue();
        INVARIANT(index < slots.size(), "WorkerThreadId {} is out of range for {} semantic operator slots", index, slots.size());
        return slots[index];
    }

    /// One blocking round trip for the slot's row. A failed transport (endpoint unreachable or
    /// non-2xx after all retries) throws, failing the query. A 2xx body that is not a chat completion
    /// counts as an unusable answer instead and comes back as an empty response, which every codec
    /// decodes to its fallback.
    [[nodiscard]] std::string roundTrip(SemanticSlot& slot)
    {
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
        return std::move(response).value_or(std::string{});
    }

    Codec codec;

private:
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
    CompletionRequest request;
    std::vector<std::string> inputNames;
    size_t numberOfAnswers;
    std::counting_semaphore<> inFlight;
    std::vector<SemanticSlot> slots;
};

}
