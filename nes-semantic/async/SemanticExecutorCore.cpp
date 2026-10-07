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

#include <SemanticExecutorCore.hpp>

#include <algorithm>
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
#include <utility>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticBackend.hpp>
#include <SemanticBackendFactory.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES::detail
{

namespace
{

/// Same bound the synchronous operators use: long enough for any handshake, short enough to fail
/// a dead host quickly.
constexpr std::chrono::milliseconds ConnectTimeout{10000};

/// The executor runs on the worker, which is where the credential lives — the configuration that
/// travelled here names only the environment variable. Resolved once, when the consumer source
/// opens, not per request.
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

/// Resolves a declared field name against a layout, reading it as an SQL identifier so that the
/// name the model was declared with finds the field the schema stores.
size_t
requireField(const AsyncRecordLayout& layout, const std::string& name, const std::string_view operatorName, const std::string_view role)
{
    const auto index = layout.indexOfColumn(name);
    if (!index.has_value())
    {
        throw InvalidSemanticModel(
            "{} {} field '{}' is not part of the schema handed to the asynchronous executor", operatorName, role, name);
    }
    return index.value();
}

SemanticMapAsyncPayload decodePayload(const AsyncOperatorContext& context, const std::string_view operatorName)
{
    const auto encoded = context.config.find(std::string{SemanticMapConfigKey});
    if (encoded == context.config.end())
    {
        throw InvalidSemanticModel("The asynchronous {} executor was configured without a '{}' entry", operatorName, SemanticMapConfigKey);
    }
    return decodeSemanticMapPayload(encoded->second);
}

}

SemanticExecutorCore::SemanticExecutorCore(const AsyncOperatorContext& context, const std::string_view operatorName)
    : SemanticExecutorCore(context, operatorName, decodePayload(context, operatorName))
{
}

SemanticExecutorCore::SemanticExecutorCore(
    const AsyncOperatorContext& context, const std::string_view operatorName, SemanticMapAsyncPayload payload)
    : config(std::move(payload.config))
{
    INVARIANT(context.inputLayout != nullptr && context.outputLayout != nullptr, "The asynchronous executor context carries no layouts");
    const auto mapSteps = static_cast<size_t>(
        std::ranges::count(config.steps, SemanticStep::Kind::MAP, [](const SemanticStep& step) { return step.kind; }));
    if (payload.outputFields.size() != mapSteps)
    {
        throw InvalidSemanticModel("{} declares {} OUTPUT fields but {} map steps", operatorName, payload.outputFields.size(), mapSteps);
    }

    apiKey = resolveApiKey(config);
    for (const auto& name : payload.inputFields)
    {
        inputFieldIndices.push_back(requireField(*context.inputLayout, name, operatorName, "INPUT"));
        inputFieldNames.push_back(context.inputLayout->nameOf(inputFieldIndices.back()));
    }
    for (const auto& name : payload.outputFields)
    {
        outputFieldIndices.push_back(requireField(*context.outputLayout, name, operatorName, "OUTPUT"));
    }
    NES_DEBUG(
        "Asynchronous {} executor ready for model '{}' at {} with {} input and {} output fields",
        operatorName,
        config.modelName,
        config.endpoint,
        inputFieldIndices.size(),
        outputFieldIndices.size());
}

SemanticExecutorCore::~SemanticExecutorCore() = default;

std::vector<RowPayload> SemanticExecutorCore::rowsOf(const std::span<const AsyncRecordView> batch) const
{
    std::vector<RowPayload> rows;
    rows.reserve(batch.size());
    for (size_t record = 0; record < batch.size(); ++record)
    {
        RowPayload row{.rowId = "row" + std::to_string(record + 1), .fields = {}};
        row.fields.reserve(inputFieldIndices.size());
        for (size_t field = 0; field < inputFieldIndices.size(); ++field)
        {
            row.fields.emplace_back(inputFieldNames[field], batch[record].readAsText(inputFieldIndices[field]));
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

std::string SemanticExecutorCore::complete(std::string prompt)
{
    auto response = backendForThisThread().complete(CompletionRequest{
        .prompt = std::move(prompt),
        .modelName = config.modelName,
        .timeout = config.requestTimeout,
        .connectTimeout = ConnectTimeout,
        .maxRetries = config.maxRetries});
    if (!response.has_value())
    {
        throw InferenceRuntimeFailure("Semantic model '{}': {}", config.modelName, response.error().message);
    }
    return std::move(response).value();
}

SemanticBackend& SemanticExecutorCore::backendForThisThread()
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

}
