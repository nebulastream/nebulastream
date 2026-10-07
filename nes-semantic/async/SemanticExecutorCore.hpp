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

#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticBackend.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES::detail
{

/// What the asynchronous semantic executors share: everything but the codec. Decodes the payload
/// `SemanticAsyncWiring` encoded, resolves the API key on the worker, locates the declared fields in
/// the layouts and owns one backend per calling thread.
///
/// Immovable, because of the mutex; executors construct it in place inside their own state.
class SemanticExecutorCore
{
public:
    /// `operatorName` ("SEM_MAP", "SEM_FILTER") only labels error messages.
    SemanticExecutorCore(const AsyncOperatorContext& context, std::string_view operatorName);

    SemanticExecutorCore(const SemanticExecutorCore&) = delete;
    SemanticExecutorCore& operator=(const SemanticExecutorCore&) = delete;
    SemanticExecutorCore(SemanticExecutorCore&&) = delete;
    SemanticExecutorCore& operator=(SemanticExecutorCore&&) = delete;
    ~SemanticExecutorCore();

    /// One row per record, ids "row1".."rowN" — unique within one prompt, which is all they need to
    /// be, and the form the sysprompts' own examples show.
    [[nodiscard]] std::vector<RowPayload> rowsOf(std::span<const AsyncRecordView> batch) const;

    /// One round trip on this thread's backend. A failed transport throws `InferenceRuntimeFailure`:
    /// unlike an unusable answer, which the codec handles, it is not something the next record would
    /// survive either.
    [[nodiscard]] std::string complete(std::string prompt);

    [[nodiscard]] const SemanticModelConfig& getConfig() const { return config; }

    /// Positions of the declared OUTPUT fields in the produced records, one per MAP step.
    [[nodiscard]] const std::vector<size_t>& getOutputFieldIndices() const { return outputFieldIndices; }

private:
    SemanticExecutorCore(const AsyncOperatorContext& context, std::string_view operatorName, SemanticMapAsyncPayload payload);

    SemanticBackend& backendForThisThread();

    SemanticModelConfig config;
    std::optional<std::string> apiKey;
    /// Positions of the declared INPUT fields in the incoming records, in declared order.
    std::vector<size_t> inputFieldIndices;
    /// Field names as the prompt spells them, kept next to the indices so building a row payload
    /// is a lookup and not a string operation.
    std::vector<std::string> inputFieldNames;
    std::vector<size_t> outputFieldIndices;

    /// An HTTP backend owns a curl handle and is not thread-safe, while `process` may run on as
    /// many threads as `maxConcurrency` allows. One backend per calling thread, created on first
    /// use: the framework owns the threads, so their number is not known here.
    std::mutex backendsMutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SemanticBackend>> backends;
};

}
