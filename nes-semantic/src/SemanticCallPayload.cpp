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

#include <SemanticCallPayload.hpp>

#include <algorithm>

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

/// Outside the anonymous namespace: reflect-cpp counts its fields through a conversion it needs to link against.
namespace detail
{
struct EncodedKeyed
{
    int64_t kind;
    std::string keyBy;
    std::string initPrompt;
    std::string initialState;
    std::string strategy;
    double threshold;
    int64_t maxActiveWindows;
    int64_t maxExamples;
    std::string timestampField;
    int64_t windowsPerCall;
};
}

using detail::EncodedKeyed;

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
    std::string model;
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
    int64_t maxTokens;
    std::optional<std::string> apiKeyEnvVar;
    std::string backend;
    bool fusion;
    std::vector<std::string> inputFields;
    std::vector<std::string> outputFields;
    /// Only written for SEM_JOIN, so the payloads (and EXPLAIN hashes) of SEM_MAP and SEM_FILTER stay as they were.
    std::optional<bool> join;
    /// Only written for SEM_AGG, for the same reason.
    std::optional<bool> agg;
    /// Only written for SEM_SCAN and SEM_WINDOW, for the same reason.
    std::optional<EncodedKeyed> keyed;
    std::optional<std::string> stateId;
    /// Only written when set, for the same reason.
    std::optional<bool> retryMissingRows;
    std::optional<int64_t> sharedConcurrency;
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

EncodedKeyed encodeKeyed(const SemanticKeyedConfig& keyed)
{
    return EncodedKeyed{
        .kind = static_cast<int64_t>(keyed.kind),
        .keyBy = keyed.keyBy,
        .initPrompt = keyed.initPrompt,
        .initialState = keyed.initialState,
        .strategy = keyed.strategy,
        .threshold = keyed.threshold,
        .maxActiveWindows = static_cast<int64_t>(keyed.maxActiveWindows),
        .maxExamples = static_cast<int64_t>(keyed.maxExamples),
        .timestampField = keyed.timestampField,
        .windowsPerCall = static_cast<int64_t>(keyed.windowsPerCall)};
}

SemanticKeyedConfig decodeKeyed(EncodedKeyed encoded)
{
    return SemanticKeyedConfig{
        .kind = readEnum(encoded.kind, SemanticKeyedConfig::Kind::WINDOW, "SemanticKeyedConfig::Kind"),
        .keyBy = std::move(encoded.keyBy),
        .initPrompt = std::move(encoded.initPrompt),
        .initialState = std::move(encoded.initialState),
        .strategy = std::move(encoded.strategy),
        .threshold = encoded.threshold,
        .maxActiveWindows = static_cast<size_t>(std::max<int64_t>(0, encoded.maxActiveWindows)),
        .maxExamples = static_cast<size_t>(std::max<int64_t>(0, encoded.maxExamples)),
        .timestampField = std::move(encoded.timestampField),
        .windowsPerCall = static_cast<size_t>(std::max<int64_t>(0, encoded.windowsPerCall))};
}

}

std::string encodeSemanticKeyedConfig(const SemanticKeyedConfig& config)
{
    return rfl::json::write(encodeKeyed(config));
}

SemanticKeyedConfig decodeSemanticKeyedConfig(const std::string_view encoded)
{
    const auto parsed = rfl::json::read<EncodedKeyed>(std::string{encoded});
    if (!parsed)
    {
        throw CannotDeserialize("Cannot read the keyed semantic configuration: {}", parsed.error().what());
    }
    return decodeKeyed(parsed.value());
}

std::string encodeSemanticCallPayload(const SemanticCallPayload& payload)
{
    const auto& config = payload.config;

    std::vector<EncodedStep> steps;
    steps.reserve(config.steps.size());
    for (const auto& step : config.steps)
    {
        steps.emplace_back(
            EncodedStep{
                .kind = static_cast<int64_t>(step.kind),
                .prompt = step.prompt,
                .outputColumn = step.outputColumn,
                .outputValues = step.outputValues,
                .defaultValue = step.defaultValue});
    }

    return rfl::json::write(
        EncodedPayload{
            .model = payload.model,
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
            .maxTokens = static_cast<int64_t>(config.maxTokens),
            .apiKeyEnvVar = config.apiKeyEnvVar,
            .backend = config.backend,
            .fusion = config.fusion,
            .inputFields = payload.inputFields,
            .outputFields = payload.outputFields,
            .join = payload.join ? std::optional{true} : std::nullopt,
            .agg = payload.agg ? std::optional{true} : std::nullopt,
            .keyed = config.keyed ? std::optional{encodeKeyed(*config.keyed)} : std::nullopt,
            .stateId = payload.stateId.empty() ? std::nullopt : std::optional{payload.stateId},
            .retryMissingRows = config.retryMissingRows ? std::optional{true} : std::nullopt,
            .sharedConcurrency
            = config.sharedConcurrency > 0 ? std::optional{static_cast<int64_t>(config.sharedConcurrency)} : std::nullopt});
}

SemanticCallPayload decodeSemanticCallPayload(const std::string_view encoded)
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
        steps.emplace_back(
            SemanticStep{
                .kind = readEnum(step.kind, SemanticStep::Kind::FILTER, "SemanticStep::Kind"),
                .prompt = std::move(step.prompt),
                .outputColumn = std::move(step.outputColumn),
                .outputValues = std::move(step.outputValues),
                .defaultValue = std::move(step.defaultValue)});
    }

    return SemanticCallPayload{
        .model = std::move(value.model),
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
            .maxTokens = static_cast<size_t>(value.maxTokens),
            .apiKeyEnvVar = std::move(value.apiKeyEnvVar),
            .backend = std::move(value.backend),
            /// SEM_CALL runs on the async framework whatever the model declares.
            .execution = SemanticExecution::ASYNCHRONOUS,
            .fusion = value.fusion,
            .retryMissingRows = value.retryMissingRows.value_or(false),
            .sharedConcurrency = static_cast<size_t>(value.sharedConcurrency.value_or(0)),
            .keyed = value.keyed ? std::optional{decodeKeyed(std::move(*value.keyed))} : std::nullopt},
        .inputFields = std::move(value.inputFields),
        .outputFields = std::move(value.outputFields),
        .join = value.join.value_or(false),
        .agg = value.agg.value_or(false),
        .stateId = std::move(value.stateId).value_or("")};
}

}
