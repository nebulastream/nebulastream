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

#include <SemanticAsyncWiring.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// A plain mirror of the configuration: reflect-cpp writes durations and enums as whatever
/// their representation happens to be, which is not something a wire format should depend on.
/// Spelling them out as integers here keeps the encoding readable and stable.
struct EncodedStep
{
    int64_t kind;
    std::string prompt;
    std::string outputColumn;
    std::vector<std::string> outputValues;
    std::string defaultValue;
};

struct EncodedPayload
{
    std::string endpoint;
    std::string modelName;
    std::string datasetPrompt;
    std::vector<EncodedStep> steps;
    int64_t payloadFormat;
    int64_t batchSize;
    int64_t maxConcurrency;
    int64_t maxRetries;
    int64_t maxWaitTimeMs;
    int64_t requestTimeoutSeconds;
    std::optional<std::string> apiKeyEnvVar;
    std::string backend;
    bool fusion;
    std::vector<std::string> inputFields;
    std::vector<std::string> outputFields;
};

/// The encoded form comes from a plan we generated ourselves, but it has been through
/// serialization, so an out-of-range enumerator is possible and must not reach a switch.
template <typename Enum>
Enum readEnum(const int64_t raw, const Enum last, const std::string_view what)
{
    if (raw < 0 || raw > static_cast<int64_t>(last))
    {
        throw CannotDeserialize("Cannot read {}: value {} is out of range", what, raw);
    }
    return static_cast<Enum>(raw);
}

}

std::string encodeSemanticMapPayload(const SemanticMapAsyncPayload& payload)
{
    const auto& config = payload.config;

    std::vector<EncodedStep> steps;
    steps.reserve(config.steps.size());
    for (const auto& step : config.steps)
    {
        steps.emplace_back(EncodedStep{
            .kind = static_cast<int64_t>(step.kind),
            .prompt = step.prompt,
            .outputColumn = step.outputColumn,
            .outputValues = step.outputValues,
            .defaultValue = step.defaultValue});
    }

    return rfl::json::write(EncodedPayload{
        .endpoint = config.endpoint,
        .modelName = config.modelName,
        .datasetPrompt = config.datasetPrompt,
        .steps = std::move(steps),
        .payloadFormat = static_cast<int64_t>(config.payloadFormat),
        .batchSize = static_cast<int64_t>(config.batchSize),
        .maxConcurrency = static_cast<int64_t>(config.maxConcurrency),
        .maxRetries = static_cast<int64_t>(config.maxRetries),
        .maxWaitTimeMs = static_cast<int64_t>(config.maxWaitTime.count()),
        .requestTimeoutSeconds = static_cast<int64_t>(config.requestTimeout.count()),
        .apiKeyEnvVar = config.apiKeyEnvVar,
        .backend = config.backend,
        .fusion = config.fusion,
        .inputFields = payload.inputFields,
        .outputFields = payload.outputFields});
}

SemanticMapAsyncPayload decodeSemanticMapPayload(const std::string_view encoded)
{
    const auto parsed = rfl::json::read<EncodedPayload>(std::string{encoded});
    if (!parsed)
    {
        throw CannotDeserialize("Cannot read the semantic model configuration: {}", parsed.error().what());
    }
    auto value = parsed.value();

    std::vector<SemanticStep> steps;
    steps.reserve(value.steps.size());
    for (auto& step : value.steps)
    {
        steps.emplace_back(SemanticStep{
            .kind = readEnum(step.kind, SemanticStep::Kind::FILTER, "SemanticStep::Kind"),
            .prompt = std::move(step.prompt),
            .outputColumn = std::move(step.outputColumn),
            .outputValues = std::move(step.outputValues),
            .defaultValue = std::move(step.defaultValue)});
    }

    return SemanticMapAsyncPayload{
        .config = SemanticModelConfig{
            .endpoint = std::move(value.endpoint),
            .modelName = std::move(value.modelName),
            .datasetPrompt = std::move(value.datasetPrompt),
            .steps = std::move(steps),
            .payloadFormat = readEnum(value.payloadFormat, PayloadFormat::JSON_OBJECT, "PayloadFormat"),
            .batchSize = static_cast<size_t>(value.batchSize),
            .maxConcurrency = static_cast<size_t>(value.maxConcurrency),
            .maxRetries = static_cast<size_t>(value.maxRetries),
            .maxWaitTime = std::chrono::milliseconds{value.maxWaitTimeMs},
            .requestTimeout = std::chrono::seconds{value.requestTimeoutSeconds},
            .apiKeyEnvVar = std::move(value.apiKeyEnvVar),
            .backend = std::move(value.backend),
            /// Only the synchronous path consults this; the executor exists because the
            /// decision was already made. Recorded so a round trip stays lossless.
            .execution = SemanticExecution::ASYNCHRONOUS,
            .fusion = value.fusion},
        .inputFields = std::move(value.inputFields),
        .outputFields = std::move(value.outputFields)};
}

}
