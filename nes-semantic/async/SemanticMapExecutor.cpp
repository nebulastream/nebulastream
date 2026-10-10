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
#include <exception>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Http/HttpBackend.hpp>
#include <Http/HttpEventLoop.hpp>
#include <Util/Logger/Logger.hpp>
#include <ChatCompletionProtocol.hpp>
#include <ErrorHandling.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticBackend.hpp>
#include <SemanticLatencyReport.hpp>
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
    /// Spelled out because the members below have to be constructed in a particular order, and
    /// because the codec takes a reference to the configuration next to it.
    SemanticMapExecutorState(SemanticModelConfig modelConfig, const size_t loopThreads)
        : config(std::move(modelConfig)), codec(config), url(fmt::format("{}/chat/completions", config.endpoint)), loop(loopThreads)
    {
    }

    SemanticModelConfig config;
    SemanticMapCodec codec;
    /// Only for the messages; the request itself is built from the endpoint.
    std::string url;
    std::optional<std::string> apiKey;
    /// Positions of the declared INPUT fields in the incoming records, in declared order.
    std::vector<size_t> inputFieldIndices;
    /// Positions of the declared OUTPUT fields in the produced records, one per step.
    std::vector<size_t> outputFieldIndices;
    /// Field names as the prompt spells them, kept next to the indices so building a row
    /// payload is a lookup and not a string operation.
    std::vector<std::string> inputFieldNames;
    /// Set when the model is answered by the mock backend, in which case nothing leaves the
    /// process. Read-only after construction, so one instance serves every call.
    std::unique_ptr<MockSemanticBackend> mock;

    /// Declared last so it is destroyed first: stopping the loop answers everything in flight, and
    /// those completions read the members above.
    HttpEventLoop loop;
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

    /// One loop thread is enough: a call waits about a second, so even at the configured 256 the
    /// answers arrive a few hundred times a second, and decoding one is a few microseconds of JSON.
    constexpr size_t LoopThreads = 1;
    state = std::make_unique<detail::SemanticMapExecutorState>(payload.config, LoopThreads);
    state->apiKey = resolveApiKey(payload.config);

    if (payload.config.backend == "mock")
    {
        /// The same construction SemanticBackendFactory performs for the blocking path: the
        /// behaviour is the endpoint string, and the output columns come from the declared steps.
        std::vector<std::string> outputColumns;
        outputColumns.reserve(payload.config.steps.size());
        for (const auto& step : payload.config.steps)
        {
            outputColumns.push_back(step.outputColumn);
        }
        state->mock = std::make_unique<MockSemanticBackend>(payload.config.endpoint, std::move(outputColumns));
        NES_DEBUG("Asynchronous SEM_MAP is answered by the mock backend ('{}'); nothing leaves the process", payload.config.endpoint);
    }
    else if (payload.config.backend != "http")
    {
        throw InvalidSemanticModel("Unknown semantic model backend '{}' (expected http or mock)", payload.config.backend);
    }

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

SemanticMapExecutor::~SemanticMapExecutor()
{
    /// The consumer source destroys the executor when the query stops, which makes this one
    /// summary line per query rather than per process.
    reportAndResetSemanticLatency("asynchronous");
}

namespace
{

/// Turns one model answer into the batch's results, or the failure into something the framework can
/// rethrow on the source thread.
void finishBatch(
    detail::SemanticMapExecutorState& state,
    const std::expected<std::string, BackendError>& response,
    const std::vector<RowPayload>& rows,
    const std::span<const AsyncRecordView> batch,
    AsyncBatchCompletion done)
{
    if (!response.has_value())
    {
        /// Unlike an unusable answer, which the codec turns into the step's default value, a failed
        /// transport is not something the next record would survive either. Thrown on the source
        /// thread by the framework, with the same error code the synchronous operator produces.
        done(std::unexpected{
            std::make_exception_ptr(InferenceRuntimeFailure("Semantic model '{}': {}", state.config.modelName, response.error().message))});
        return;
    }

    /// Never fails and always returns one entry per row, each holding one answer per step.
    const auto answers = state.codec.parse(response.value(), rows);
    std::vector<AsyncRecordResult> results(batch.size());
    for (size_t record = 0; record < batch.size(); ++record)
    {
        auto& fields = results[record].fields;
        fields.reserve(state.outputFieldIndices.size());
        for (size_t step = 0; step < state.outputFieldIndices.size(); ++step)
        {
            fields.emplace_back(AsyncFieldValue{.fieldIndex = state.outputFieldIndices[step], .value = answers[record][step]});
        }
    }
    done(std::move(results));
}

}

void SemanticMapExecutor::submit(const std::span<const AsyncRecordView> batch, AsyncBatchCompletion done)
{
    if (batch.empty())
    {
        done(std::vector<AsyncRecordResult>{});
        return;
    }

    /// Row ids are what ties an answer back to its record; they only have to be unique within
    /// one prompt, and "rowN" is the form the sysprompt's own example shows. Held through a
    /// shared_ptr because the codec needs them again when the answer arrives, which is on another
    /// thread and at another time.
    auto rows = std::make_shared<std::vector<RowPayload>>();
    rows->reserve(batch.size());
    for (size_t record = 0; record < batch.size(); ++record)
    {
        RowPayload row{.rowId = "row" + std::to_string(record + 1), .fields = {}};
        row.fields.reserve(state->inputFieldIndices.size());
        for (size_t field = 0; field < state->inputFieldIndices.size(); ++field)
        {
            row.fields.emplace_back(state->inputFieldNames[field], batch[record].readAsText(state->inputFieldIndices[field]));
        }
        rows->push_back(std::move(row));
    }

    const CompletionRequest request{
        .prompt = state->codec.buildPrompt(*rows),
        .modelName = state->config.modelName,
        .timeout = state->config.requestTimeout,
        .connectTimeout = CompletionRequest{}.connectTimeout,
        .maxRetries = state->config.maxRetries};

    if (state->mock != nullptr)
    {
        /// Waited out on the loop's timer rather than slept through: this thread is driving every
        /// other call in flight.
        state->loop.after(
            state->mock->configuredDelay(),
            [this, rows, batch, request, done = std::move(done)]() mutable
            { finishBatch(*state, state->mock->answer(request), *rows, batch, std::move(done)); });
        return;
    }

    state->loop.send(
        chatCompletionRequest(state->config.endpoint, state->apiKey, request),
        [this, rows, batch, done = std::move(done)](std::expected<HttpResponse, HttpError> exchange) mutable
        { finishBatch(*state, chatCompletionOutcome(state->url, exchange), *rows, batch, std::move(done)); });
}

}
