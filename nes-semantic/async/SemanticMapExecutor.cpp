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

#include <Async/SemanticMapExecutor.hpp>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticBackend.hpp>
#include <SemanticBackendFactory.hpp>
#include <SemanticMapCodec.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Same bound the synchronous operator uses: long enough for any handshake, short enough to
/// fail a dead host quickly.
constexpr std::chrono::milliseconds ConnectTimeout{10000};

/// The executor runs on the worker, which is where the credential lives — the configuration
/// that travelled here names only the environment variable. Resolved once, when the consumer
/// source opens, not per request.
std::optional<std::string> resolveApiKey(const SemanticModelConfig& config)
{
    if (!config.apiKeyEnvVar.has_value())
    {
        return std::nullopt;
    }
    const char* const value = std::getenv(config.apiKeyEnvVar->c_str());
    if (value == nullptr)
    {
        throw InvalidSemanticModel(
            "Semantic model reads its API key from environment variable {}, which is not set on this worker", *config.apiKeyEnvVar);
    }
    return std::string{value};
}

/// Resolves a declared field name against a layout, reading it as an SQL identifier so that
/// the name the model was declared with finds the field the schema stores.
size_t requireField(const AsyncRecordLayout& layout, const std::string& name, const std::string_view role)
{
    const auto index = layout.indexOfColumn(name);
    if (!index.has_value())
    {
        throw InvalidSemanticModel("SEM_MAP {} field '{}' is not part of the schema handed to the asynchronous executor", role, name);
    }
    return index.value();
}

}

namespace detail
{

struct SemanticMapExecutorState
{
    /// Spelled out because the mutex below makes this struct immovable, so it has to be
    /// constructed in place rather than assigned from an aggregate initializer.
    explicit SemanticMapExecutorState(SemanticModelConfig modelConfig)
        : config(std::move(modelConfig)), codec(config)
    {
    }

    SemanticModelConfig config;
    SemanticMapCodec codec;
    std::optional<std::string> apiKey;
    /// Positions of the declared INPUT fields in the incoming records, in declared order.
    std::vector<size_t> inputFieldIndices;
    /// Positions of the declared OUTPUT fields in the produced records, one per step.
    std::vector<size_t> outputFieldIndices;
    /// Field names as the prompt spells them, kept next to the indices so building a row
    /// payload is a lookup and not a string operation.
    std::vector<std::string> inputFieldNames;

    /// An HTTP backend owns a curl handle and is not thread-safe, while `process` may run on
    /// as many threads as `maxConcurrency` allows. One backend per calling thread, created on
    /// first use: the framework owns the threads, so their number is not known here.
    std::mutex backendsMutex;
    std::unordered_map<std::thread::id, std::unique_ptr<SemanticBackend>> backends;

    SemanticBackend& backendForThisThread()
    {
        const auto thisThread = std::this_thread::get_id();
        const std::lock_guard lock{backendsMutex};
        auto& backend = backends[thisThread];
        if (backend == nullptr)
        {
            backend = SemanticBackendFactory::create(config, apiKey);
        }
        return *backend;
    }
};

}

SemanticMapExecutor::SemanticMapExecutor(AsyncOperatorContext context)
{
    const auto encoded = context.config.find(std::string{SemanticMapConfigKey});
    if (encoded == context.config.end())
    {
        throw InvalidSemanticModel("The asynchronous SEM_MAP executor was configured without a '{}' entry", SemanticMapConfigKey);
    }
    INVARIANT(context.inputLayout != nullptr && context.outputLayout != nullptr, "The asynchronous executor context carries no layouts");

    auto payload = decodeSemanticMapPayload(encoded->second);
    if (payload.outputFields.size() != payload.config.steps.size())
    {
        throw InvalidSemanticModel(
            "SEM_MAP declares {} OUTPUT fields but {} steps", payload.outputFields.size(), payload.config.steps.size());
    }

    state = std::make_unique<detail::SemanticMapExecutorState>(payload.config);
    state->apiKey = resolveApiKey(payload.config);

    for (const auto& name : payload.inputFields)
    {
        state->inputFieldIndices.push_back(requireField(*context.inputLayout, name, "INPUT"));
        state->inputFieldNames.push_back(context.inputLayout->nameOf(state->inputFieldIndices.back()));
    }
    for (const auto& name : payload.outputFields)
    {
        state->outputFieldIndices.push_back(requireField(*context.outputLayout, name, "OUTPUT"));
    }

    NES_DEBUG(
        "Asynchronous SEM_MAP executor ready for model '{}' at {} with {} input and {} output fields",
        payload.config.modelName,
        payload.config.endpoint,
        state->inputFieldIndices.size(),
        state->outputFieldIndices.size());
}

SemanticMapExecutor::~SemanticMapExecutor() = default;

std::vector<AsyncRecordResult> SemanticMapExecutor::process(const std::span<const AsyncRecordView> batch)
{
    std::vector<AsyncRecordResult> results(batch.size());
    if (batch.empty())
    {
        return results;
    }

    /// Row ids are what ties an answer back to its record; they only have to be unique within
    /// one prompt, and "rowN" is the form the sysprompt's own example shows.
    std::vector<RowPayload> rows;
    rows.reserve(batch.size());
    for (size_t record = 0; record < batch.size(); ++record)
    {
        RowPayload row{.rowId = "row" + std::to_string(record + 1), .fields = {}};
        row.fields.reserve(state->inputFieldIndices.size());
        for (size_t field = 0; field < state->inputFieldIndices.size(); ++field)
        {
            row.fields.emplace_back(state->inputFieldNames[field], batch[record].readAsText(state->inputFieldIndices[field]));
        }
        rows.push_back(std::move(row));
    }

    const auto response = state->backendForThisThread().complete(
        CompletionRequest{
            .prompt = state->codec.buildPrompt(rows),
            .modelName = state->config.modelName,
            .timeout = state->config.requestTimeout,
            .connectTimeout = ConnectTimeout,
            .maxRetries = state->config.maxRetries});
    if (!response.has_value())
    {
        /// Unlike an unusable answer, which the codec turns into the step's default value, a
        /// failed transport is not something the next record would survive either.
        throw InferenceRuntimeFailure("Semantic model '{}': {}", state->config.modelName, response.error().message);
    }

    /// Never fails and always returns one entry per row, each holding one answer per step.
    const auto answers = state->codec.parse(response.value(), rows);
    for (size_t record = 0; record < batch.size(); ++record)
    {
        auto& fields = results[record].fields;
        fields.reserve(state->outputFieldIndices.size());
        for (size_t step = 0; step < state->outputFieldIndices.size(); ++step)
        {
            fields.emplace_back(AsyncFieldValue{.fieldIndex = state->outputFieldIndices[step], .value = answers[record][step]});
        }
    }
    return results;
}

}
