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

#include <SemanticModelCatalog.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticImage.hpp>

namespace NES::detail
{

struct ReflectedSemanticStep
{
    std::optional<int64_t> kind;
    std::optional<std::string> prompt;
    std::optional<std::string> outputColumn;
    std::optional<std::vector<std::string>> outputValues;
    std::optional<std::string> defaultValue;
};

struct
    ReflectedSemanticModel /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding optionals of vector-backed types trip the check; no real escape
{
    std::optional<std::string> name;
    std::optional<std::string> endpoint;
    std::optional<std::string> modelName;
    std::optional<std::string> datasetPrompt;
    std::optional<std::vector<Reflected>> steps;
    std::optional<int64_t> payloadFormat;
    std::optional<int64_t> batchSize;
    std::optional<int64_t> maxConcurrency;
    std::optional<int64_t> maxRetries;
    std::optional<int64_t> maxWaitTimeMs;
    std::optional<int64_t> requestTimeoutSeconds;
    /// Optional on read: catalogs reflected before MAX_TOKENS existed carry none, which means 0.
    std::optional<int64_t> maxTokens;
    std::optional<std::string> apiKeyEnvVar;
    std::optional<std::string> backend;
    std::optional<int64_t> execution;
    std::optional<bool> preserveOrder;
    std::optional<bool> fusion;
    /// Optional on read: catalogs reflected before RETRY_MISSING_ROWS existed carry none, which means false.
    std::optional<bool> retryMissingRows;
    /// Optional on read: catalogs reflected before SHARED_CONCURRENCY existed carry none, which means 0.
    std::optional<int64_t> sharedConcurrency;
    /// SEM_SCAN and SEM_WINDOW only, as `encodeSemanticKeyedConfig` writes it.
    std::optional<std::string> keyed;
    /// Optional on read: catalogs reflected before LLM.IMAGE_FIELDS existed carry none, which means no image inputs.
    std::optional<std::vector<std::string>> imageFields;
    std::optional<std::string> imageRoot;
    std::optional<int64_t> imageMaxBytes;
    /// Optional on read: catalogs reflected before LLM.ANSWER_MODE existed carry none, which means JSON.
    std::optional<int64_t> answerMode;
    std::optional<std::string> inputLabel;
    std::optional<SemanticFieldList> inputs;
    std::optional<SemanticFieldList> outputs;
};

}

namespace NES
{

namespace
{

/// Wire payloads are untrusted: a numeric value outside the enum's range would otherwise
/// become an invalid enumerator that every later switch silently mishandles.
template <typename Enum>
Enum unreflectEnum(const int64_t raw, const Enum last, const std::string_view what)
{
    if (raw < 0 || raw > static_cast<int64_t>(last))
    {
        throw CannotDeserialize("Failed to deserialize {}: value {} is out of range", what, raw);
    }
    return static_cast<Enum>(raw);
}

Reflected reflectStep(const SemanticStep& step, const ReflectionContext& context)
{
    return context.reflect(
        detail::ReflectedSemanticStep{
            .kind = std::make_optional(static_cast<int64_t>(step.kind)),
            .prompt = std::make_optional(step.prompt),
            .outputColumn = std::make_optional(step.outputColumn),
            .outputValues = std::make_optional(step.outputValues),
            .defaultValue = std::make_optional(step.defaultValue)});
}

SemanticStep unreflectStep(const Reflected& rfl, const ReflectionContext& context)
{
    auto reflected = context.unreflect<detail::ReflectedSemanticStep>(rfl);
    if (!reflected.kind.has_value() || !reflected.prompt.has_value() || !reflected.outputColumn.has_value()
        || !reflected.outputValues.has_value() || !reflected.defaultValue.has_value())
    {
        throw CannotDeserialize("Failed to deserialize SemanticStep");
    }
    return SemanticStep{
        .kind = unreflectEnum(reflected.kind.value(), SemanticStep::Kind::FILTER, "SemanticStep::Kind"),
        .prompt = std::move(reflected.prompt).value(),
        .outputColumn = std::move(reflected.outputColumn).value(),
        .outputValues = std::move(reflected.outputValues).value(),
        .defaultValue = std::move(reflected.defaultValue).value()};
}

/// SEM_SCAN and SEM_WINDOW: one MAP step carrying the prompt, and a fixed set of OUTPUT fields.
void validateKeyed(const std::string& name, const SemanticModelConfig& config, const SemanticModelSchema& schema)
{
    const auto& keyed = *config.keyed;
    const auto outputs = schema.outputs.size();
    if (config.steps.size() != 1 || config.steps.front().kind != SemanticStep::Kind::MAP)
    {
        throw InvalidSemanticModel("Semantic model '{}': SEM_SCAN and SEM_WINDOW models have exactly one prompt", name);
    }
    if (keyed.kind == SemanticKeyedConfig::Kind::SCAN)
    {
        if (outputs != 2)
        {
            throw InvalidSemanticModel(
                "Semantic model '{}': SEM_SCAN declares two OUTPUT fields, the state and its confidence, but got {}", name, outputs);
        }
        if (keyed.keyBy.empty())
        {
            throw InvalidSemanticModel("Semantic model '{}': SEM_SCAN needs LLM.KEY_BY, the field it folds rows by", name);
        }
    }
    else
    {
        if (outputs != 5)
        {
            throw InvalidSemanticModel(
                "Semantic model '{}': SEM_WINDOW declares five OUTPUT fields: the window id, its description, its start, the row's "
                "continuity score and whether the row opened the window; but got {}",
                name,
                outputs);
        }
        if (keyed.strategy != "summary" && keyed.strategy != "pairwise")
        {
            throw InvalidSemanticModel(
                "Semantic model '{}': LLM.STRATEGY must be 'summary' or 'pairwise', but was '{}'", name, keyed.strategy);
        }
        if (!(keyed.threshold >= 0.0 && keyed.threshold <= 1.0))
        {
            throw InvalidSemanticModel("Semantic model '{}': LLM.THRESHOLD must be between 0 and 1, but was {}", name, keyed.threshold);
        }
    }
    for (const auto& output : schema.outputs)
    {
        if (!output.getDataType().isType(DataType::Type::VARSIZED))
        {
            throw InvalidSemanticModel("Semantic model '{}': the OUTPUT fields of SEM_SCAN and SEM_WINDOW are VARSIZED", name);
        }
    }
    /// One row per prompt, as in llm_operator; a batch of keys would need a prompt with one prior state per row.
    if (config.batchSize != 1)
    {
        throw InvalidSemanticModel("Semantic model '{}': SEM_SCAN and SEM_WINDOW send one row per prompt; BATCH_SIZE must be 1", name);
    }
}

/// LLM.IMAGE_FIELDS: each one an INPUT field, named once; only SEM_MAP and SEM_FILTER, one row per prompt, never fused.
void validateImageFields(const std::string& name, const SemanticModelConfig& config, const SemanticModelSchema& schema)
{
    if (config.imageFields.empty())
    {
        return;
    }
    for (const auto& field : config.imageFields)
    {
        const bool declared = std::ranges::any_of(
            schema.inputs,
            [&field](const auto& input)
            { return static_cast<const Identifier&>(input.getFullyQualifiedName()).asCanonicalString() == field; });
        if (!declared)
        {
            throw InvalidSemanticModel(
                "Semantic model '{}': LLM.IMAGE_FIELDS names '{}', which is not one of its INPUT fields", name, field);
        }
        if (std::ranges::count(config.imageFields, field) > 1)
        {
            throw InvalidSemanticModel("Semantic model '{}': LLM.IMAGE_FIELDS names '{}' twice", name, field);
        }
    }
    if (config.keyed)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': LLM.IMAGE_FIELDS is supported for SEM_MAP and SEM_FILTER models only, not for SEM_SCAN or SEM_WINDOW",
            name);
    }
    /// One image request per row: a prompt with several rows would need the model to tell whose image is whose.
    if (config.batchSize != 1)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': a model with LLM.IMAGE_FIELDS sends one row per prompt; BATCH_SIZE must be 1", name);
    }
    /// Fusion asks several questions in one prompt over one row's text; it is not defined for image parts, so rather
    /// than ignoring an explicit FUSION TRUE, it is rejected.
    if (config.fusion)
    {
        throw InvalidSemanticModel("Semantic model '{}': a model with LLM.IMAGE_FIELDS is never fused; remove LLM.FUSION", name);
    }
    if (config.imageMaxBytes == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': LLM.IMAGE_MAX_BYTES must be at least 1", name);
    }
}

/// LLM.ANSWER_MODE 'LABEL': one SEM_MAP or SEM_FILTER step, one row per request, with values to answer from.
void validateLabelMode(const std::string& name, const SemanticModelConfig& config)
{
    if (config.answerMode != AnswerMode::LABEL)
    {
        if (!config.inputLabel.empty())
        {
            throw InvalidSemanticModel("Semantic model '{}': LLM.INPUT_LABEL requires 'LABEL' AS LLM.ANSWER_MODE", name);
        }
        return;
    }
    if (config.keyed)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': LLM.ANSWER_MODE LABEL is supported for SEM_MAP and SEM_FILTER models only, not for SEM_SCAN or "
            "SEM_WINDOW",
            name);
    }
    if (config.steps.size() != 1)
    {
        throw InvalidSemanticModel("Semantic model '{}': LLM.ANSWER_MODE LABEL asks one question per request; it has one step", name);
    }
    const auto& step = config.steps.front();
    if (step.kind == SemanticStep::Kind::MAP && step.outputValues.size() < 2)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': LLM.ANSWER_MODE LABEL answers with one of LLM.OUTPUT_VALUES; a SEM_MAP model needs at least two",
            name);
    }
    /// The prompt carries one row: the student is trained on exactly that.
    if (config.batchSize != 1)
    {
        throw InvalidSemanticModel("Semantic model '{}': LLM.ANSWER_MODE LABEL sends one row per prompt; BATCH_SIZE must be 1", name);
    }
    if (config.fusion)
    {
        throw InvalidSemanticModel("Semantic model '{}': a model with LLM.ANSWER_MODE LABEL is never fused; remove LLM.FUSION", name);
    }
    if (!config.imageFields.empty())
    {
        throw InvalidSemanticModel("Semantic model '{}': LLM.ANSWER_MODE LABEL takes text inputs only, not LLM.IMAGE_FIELDS", name);
    }
    /// The prompt is exactly the student's training prompt, which has no dataset description.
    if (!config.datasetPrompt.empty())
    {
        throw InvalidSemanticModel("Semantic model '{}': LLM.ANSWER_MODE LABEL sends no LLM.DATASET_PROMPT; remove it", name);
    }
}

}

void SemanticModelCatalog::registerModel(std::string name, SemanticModelConfig config, SemanticModelSchema schema)
{
    if (config.endpoint.empty())
    {
        throw InvalidSemanticModel("Semantic model '{}': ENDPOINT must not be empty", name);
    }
    if (config.modelName.empty())
    {
        throw InvalidSemanticModel("Semantic model '{}': MODEL_NAME must not be empty", name);
    }
    if (config.backend != "http" && config.backend != "mock")
    {
        throw InvalidSemanticModel("Semantic model '{}': BACKEND must be 'http' or 'mock', but was '{}'", name, config.backend);
    }
    if (config.backend == "mock" && !MockSemanticBackend::isValidBehaviour(config.endpoint))
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': ENDPOINT '{}' is not a mock behaviour (echo, label:<X>, unparseable or fail)", name, config.endpoint);
    }
    if (config.batchSize == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': BATCH_SIZE must be at least 1", name);
    }
    /// The synchronous operator sends one record per request. Accepting a larger value there would
    /// silently run with batch size 1, so it stays rejected; the asynchronous executor hands a
    /// whole batch to one prompt, which is what BATCH_SIZE means in the first place.
    if (config.batchSize > 1 && config.execution != SemanticExecution::ASYNCHRONOUS)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': BATCH_SIZE {} requires 'ASYNC' AS LLM.EXECUTION; the synchronous operator sends one record per request",
            name,
            config.batchSize);
    }
    if (config.maxConcurrency == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': MAX_CONCURRENCY must be at least 1", name);
    }
    /// The synchronous operator handles one record at a time on each worker thread, so its output
    /// is in input order whatever is configured. Accepting PRESERVE_ORDER false there would
    /// promise a latency gain it cannot deliver.
    if (!config.preserveOrder && config.execution != SemanticExecution::ASYNCHRONOUS)
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': PRESERVE_ORDER false requires 'ASYNC' AS LLM.EXECUTION; the synchronous operator is ordered anyway",
            name);
    }

    if (schema.inputs.size() == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': at least one INPUT field is required", name);
    }
    if (config.steps.empty())
    {
        throw InvalidSemanticModel("Semantic model '{}': at least one step is required", name);
    }

    /// A MAP step produces exactly one output column, a FILTER step none. The handler builds both
    /// sides together, so a mismatch here means the caller bypassed it.
    const auto mapSteps = static_cast<size_t>(
        std::ranges::count(config.steps, SemanticStep::Kind::MAP, [](const SemanticStep& step) { return step.kind; }));
    if (mapSteps > 0 && schema.outputs.size() == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': at least one OUTPUT field is required", name);
    }
    if (config.keyed)
    {
        validateKeyed(name, config, schema);
    }
    else if (mapSteps != schema.outputs.size())
    {
        throw InvalidSemanticModel("Semantic model '{}': {} OUTPUT field(s) but {} map step(s)", name, schema.outputs.size(), mapSteps);
    }

    for (const auto& step : config.steps)
    {
        if (step.prompt.empty())
        {
            throw InvalidSemanticModel("Semantic model '{}': PROMPT must not be empty", name);
        }
        switch (step.kind)
        {
            case SemanticStep::Kind::MAP:
                if (step.outputColumn.empty())
                {
                    throw InvalidSemanticModel("Semantic model '{}': step has no output column", name);
                }
                break;
            /// A filter keeps or drops the row; there is no column to restrict or default-fill.
            case SemanticStep::Kind::FILTER:
                if (!step.outputValues.empty())
                {
                    throw InvalidSemanticModel(
                        "Semantic model '{}': OUTPUT_VALUES requires an OUTPUT field; a filter model has none", name);
                }
                if (!step.defaultValue.empty())
                {
                    throw InvalidSemanticModel(
                        "Semantic model '{}': DEFAULT_VALUE requires an OUTPUT field; a filter model has none", name);
                }
                break;
        }
    }

    /// Stage 1 builds the prompt from raw text only; there is no numeric-to-text conversion in
    /// the physical operator yet, so non-text inputs are rejected here instead of at lowering.
    for (const auto& field : schema.inputs)
    {
        if (field.getDataType().type != DataType::Type::VARSIZED)
        {
            throw InvalidSemanticModel("Semantic model '{}' input field '{}': type must be VARSIZED", name, field.getFullyQualifiedName());
        }
    }

    /// The model answers with text, so every output field is a string. Numeric answers
    /// are the caller's job to cast downstream, exactly as the Python reference does
    /// (it stringifies every answer with `str()`).
    for (const auto& field : schema.outputs)
    {
        if (field.getDataType().type != DataType::Type::VARSIZED)
        {
            throw InvalidSemanticModel("Semantic model '{}' output field '{}': type must be VARSIZED", name, field.getFullyQualifiedName());
        }
    }

    validateImageFields(name, config, schema);
    validateLabelMode(name, config);

    /// Enforced here rather than only in the statement handler, so the optimizer tests and any
    /// embedded caller cannot silently replace an entry either.
    if (entries.contains(name))
    {
        throw SemanticModelAlreadyExists("Semantic model '{}' is already registered", name);
    }
    auto registered = RegisteredSemanticModel{name, std::move(config), std::move(schema)};
    entries.emplace(std::move(name), std::move(registered));
}

void SemanticModelCatalog::removeModel(const std::string& modelName)
{
    if (entries.erase(modelName) == 0)
    {
        throw UnknownSemanticModelName("Semantic model '{}' was never registered", modelName);
    }
}

bool SemanticModelCatalog::hasModel(const std::string& modelName) const
{
    return entries.contains(modelName);
}

std::vector<std::string> SemanticModelCatalog::getModelNames() const
{
    std::vector<std::string> names;
    names.reserve(entries.size());
    for (const auto& [name, _] : entries)
    {
        names.push_back(name);
    }
    return names;
}

std::vector<RegisteredSemanticModel> SemanticModelCatalog::getRegisteredModels() const
{
    std::vector<RegisteredSemanticModel> models;
    models.reserve(entries.size());
    for (const auto& [_, model] : entries)
    {
        models.push_back(model);
    }
    return models;
}

bool RegisteredSemanticModel::hasFilterStep() const
{
    return std::ranges::any_of(config.steps, [](const SemanticStep& step) { return step.kind == SemanticStep::Kind::FILTER; });
}

RegisteredSemanticModel fuseSemanticModels(const RegisteredSemanticModel& first, const RegisteredSemanticModel& second)
{
    auto config = first.config;
    config.steps.insert(config.steps.end(), second.config.steps.begin(), second.config.steps.end());
    /// `_make_filter_step(prompt, n)` with n counting the filters already fused, i.e. the same
    /// numbering the codecs derive; stored so that the entry describes itself.
    size_t filters = 0;
    for (auto& step : config.steps)
    {
        if (step.kind == SemanticStep::Kind::FILTER)
        {
            step.outputColumn = fmt::format("__filter_{}", filters++);
        }
    }

    std::vector<UnqualifiedUnboundField> outputs{first.schema.outputs.begin(), first.schema.outputs.end()};
    outputs.insert(outputs.end(), second.schema.outputs.begin(), second.schema.outputs.end());
    auto fusedOutputs = SemanticFieldList::tryCreateCollisionFree(std::move(outputs));
    if (!fusedOutputs.has_value())
    {
        throw InvalidSemanticModel(
            "Cannot fuse semantic models '{}' and '{}': their OUTPUT fields collide: {}",
            first.name,
            second.name,
            SemanticFieldList::createCollisionString(fusedOutputs.error()));
    }

    return RegisteredSemanticModel{
        fmt::format("{}+{}", first.name, second.name),
        std::move(config),
        SemanticModelSchema{.inputs = first.schema.inputs, .outputs = std::move(fusedOutputs).value()}};
}

RegisteredSemanticModel SemanticModelCatalog::load(const std::string& modelName) const
{
    if (auto it = entries.find(modelName); it != entries.end())
    {
        return it->second;
    }
    throw UnknownSemanticModelName("Semantic model '{}' was never registered", modelName);
}

Reflected Reflector<RegisteredSemanticModel>::operator()(const RegisteredSemanticModel& model, const ReflectionContext& context) const
{
    const auto& config = model.getConfig();

    std::vector<Reflected> reflectedSteps;
    reflectedSteps.reserve(config.steps.size());
    for (const auto& step : config.steps)
    {
        reflectedSteps.push_back(reflectStep(step, context));
    }

    return context.reflect(
        detail::ReflectedSemanticModel{
            .name = std::make_optional(model.getName()),
            .endpoint = std::make_optional(config.endpoint),
            .modelName = std::make_optional(config.modelName),
            .datasetPrompt = std::make_optional(config.datasetPrompt),
            .steps = std::make_optional(std::move(reflectedSteps)),
            .payloadFormat = std::make_optional(static_cast<int64_t>(config.payloadFormat)),
            .batchSize = std::make_optional(static_cast<int64_t>(config.batchSize)),
            .maxConcurrency = std::make_optional(static_cast<int64_t>(config.maxConcurrency)),
            .maxRetries = std::make_optional(static_cast<int64_t>(config.maxRetries)),
            .maxWaitTimeMs = std::make_optional(static_cast<int64_t>(config.maxWaitTime.count())),
            .requestTimeoutSeconds = std::make_optional(static_cast<int64_t>(config.requestTimeout.count())),
            .maxTokens = std::make_optional(static_cast<int64_t>(config.maxTokens)),
            .apiKeyEnvVar = config.apiKeyEnvVar,
            .backend = std::make_optional(config.backend),
            .execution = std::make_optional(static_cast<int64_t>(config.execution)),
            .preserveOrder = std::make_optional(config.preserveOrder),
            .fusion = std::make_optional(config.fusion),
            .retryMissingRows = std::make_optional(config.retryMissingRows),
            .sharedConcurrency = std::make_optional(static_cast<int64_t>(config.sharedConcurrency)),
            .keyed = config.keyed ? std::optional{encodeSemanticKeyedConfig(*config.keyed)} : std::nullopt,
            .imageFields = std::make_optional(config.imageFields),
            .imageRoot = std::make_optional(config.imageRoot),
            .imageMaxBytes = std::make_optional(static_cast<int64_t>(config.imageMaxBytes)),
            .answerMode = std::make_optional(static_cast<int64_t>(config.answerMode)),
            .inputLabel = std::make_optional(config.inputLabel),
            .inputs = std::make_optional(model.getSchema().inputs),
            .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredSemanticModel Unreflector<RegisteredSemanticModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedSemanticModel>(rfl);
    if (!reflected.name.has_value() || !reflected.endpoint.has_value() || !reflected.modelName.has_value()
        || !reflected.datasetPrompt.has_value() || !reflected.steps.has_value() || !reflected.payloadFormat.has_value()
        || !reflected.batchSize.has_value() || !reflected.maxConcurrency.has_value() || !reflected.maxRetries.has_value()
        || !reflected.maxWaitTimeMs.has_value() || !reflected.requestTimeoutSeconds.has_value() || !reflected.backend.has_value()
        || !reflected.execution.has_value() || !reflected.preserveOrder.has_value() || !reflected.fusion.has_value()
        || !reflected.inputs.has_value() || !reflected.outputs.has_value())
    {
        throw CannotDeserialize("Failed to deserialize RegisteredSemanticModel");
    }

    std::vector<SemanticStep> steps;
    steps.reserve(reflected.steps->size());
    for (const auto& reflectedStep : reflected.steps.value())
    {
        steps.push_back(unreflectStep(reflectedStep, context));
    }

    SemanticModelConfig config{
        .endpoint = std::move(reflected.endpoint).value(),
        .modelName = std::move(reflected.modelName).value(),
        .datasetPrompt = std::move(reflected.datasetPrompt).value(),
        .steps = std::move(steps),
        .payloadFormat = unreflectEnum(reflected.payloadFormat.value(), PayloadFormat::JSON_OBJECT, "PayloadFormat"),
        .batchSize = static_cast<size_t>(reflected.batchSize.value()),
        .maxConcurrency = static_cast<size_t>(reflected.maxConcurrency.value()),
        .maxRetries = static_cast<size_t>(reflected.maxRetries.value()),
        .maxWaitTime = std::chrono::milliseconds{reflected.maxWaitTimeMs.value()},
        .requestTimeout = std::chrono::seconds{reflected.requestTimeoutSeconds.value()},
        .maxTokens = static_cast<size_t>(reflected.maxTokens.value_or(0)),
        .apiKeyEnvVar = reflected.apiKeyEnvVar,
        .backend = std::move(reflected.backend).value(),
        .execution = unreflectEnum(reflected.execution.value(), SemanticExecution::ASYNCHRONOUS, "SemanticExecution"),
        .preserveOrder = reflected.preserveOrder.value(),
        .fusion = reflected.fusion.value(),
        .retryMissingRows = reflected.retryMissingRows.value_or(false),
        .sharedConcurrency = static_cast<size_t>(reflected.sharedConcurrency.value_or(0)),
        .keyed = reflected.keyed ? std::optional{decodeSemanticKeyedConfig(*reflected.keyed)} : std::nullopt,
        .imageFields = std::move(reflected.imageFields).value_or(std::vector<std::string>{}),
        .imageRoot = std::move(reflected.imageRoot).value_or(""),
        .imageMaxBytes = static_cast<size_t>(reflected.imageMaxBytes.value_or(static_cast<int64_t>(DefaultImageMaxBytes))),
        .answerMode = unreflectEnum(reflected.answerMode.value_or(0), AnswerMode::LABEL, "AnswerMode"),
        .inputLabel = std::move(reflected.inputLabel).value_or("")};

    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the
    /// reflected form. Schema's user-declared destructor suppresses its implicit move ctor,
    /// so the schema fields are passed by value.
    SemanticModelSchema schema{.inputs = reflected.inputs.value(), .outputs = reflected.outputs.value()};
    return RegisteredSemanticModel{std::move(reflected.name).value(), std::move(config), std::move(schema)};
}

}
