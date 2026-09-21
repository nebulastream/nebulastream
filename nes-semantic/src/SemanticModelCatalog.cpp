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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <ErrorHandling.hpp>
#include <Util/Reflection.hpp>

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
    std::optional<std::string> apiKeyEnvVar;
    std::optional<std::string> backend;
    std::optional<SemanticFieldList> inputs;
    std::optional<SemanticFieldList> outputs;
};

}

namespace NES
{

namespace
{

Reflected reflectStep(const SemanticStep& step, const ReflectionContext& context)
{
    return context.reflect(detail::ReflectedSemanticStep{
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
        .kind = static_cast<SemanticStep::Kind>(reflected.kind.value()),
        .prompt = std::move(reflected.prompt).value(),
        .outputColumn = std::move(reflected.outputColumn).value(),
        .outputValues = std::move(reflected.outputValues).value(),
        .defaultValue = std::move(reflected.defaultValue).value()};
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
        throw InvalidSemanticModel("Semantic model '{}': MODEL must not be empty", name);
    }
    if (config.backend != "http" && config.backend != "mock")
    {
        throw InvalidSemanticModel("Semantic model '{}': BACKEND must be 'http' or 'mock', but was '{}'", name, config.backend);
    }
    if (config.batchSize == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': BATCH_SIZE must be at least 1", name);
    }
    if (config.maxConcurrency == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': MAX_CONCURRENCY must be at least 1", name);
    }

    if (schema.inputs.size() == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': at least one INPUT field is required", name);
    }
    if (schema.outputs.size() == 0)
    {
        throw InvalidSemanticModel("Semantic model '{}': at least one OUTPUT field is required", name);
    }

    /// One step produces exactly one output column. The binder builds both sides together,
    /// so a mismatch here means the caller bypassed it.
    if (config.steps.size() != schema.outputs.size())
    {
        throw InvalidSemanticModel(
            "Semantic model '{}': {} OUTPUT field(s) but {} step(s)", name, schema.outputs.size(), config.steps.size());
    }

    for (const auto& step : config.steps)
    {
        if (step.prompt.empty())
        {
            throw InvalidSemanticModel("Semantic model '{}': PROMPT must not be empty", name);
        }
        if (step.outputColumn.empty())
        {
            throw InvalidSemanticModel("Semantic model '{}': step has no output column", name);
        }
    }

    /// The model answers with text, so every output field is a string. Numeric answers
    /// are the caller's job to cast downstream, exactly as the Python reference does
    /// (it stringifies every answer with `str()`).
    for (const auto& field : schema.outputs)
    {
        if (field.getDataType().type != DataType::Type::VARSIZED)
        {
            throw InvalidSemanticModel(
                "Semantic model '{}' output field '{}': type must be VARSIZED", name, field.getFullyQualifiedName());
        }
    }

    auto registered = RegisteredSemanticModel{name, std::move(config), std::move(schema)};
    entries.insert_or_assign(std::move(name), std::move(registered));
}

void SemanticModelCatalog::removeModel(const std::string& modelName)
{
    entries.erase(modelName);
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

    return context.reflect(detail::ReflectedSemanticModel{
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
        .apiKeyEnvVar = config.apiKeyEnvVar,
        .backend = std::make_optional(config.backend),
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
        .payloadFormat = static_cast<PayloadFormat>(reflected.payloadFormat.value()),
        .batchSize = static_cast<size_t>(reflected.batchSize.value()),
        .maxConcurrency = static_cast<size_t>(reflected.maxConcurrency.value()),
        .maxRetries = static_cast<size_t>(reflected.maxRetries.value()),
        .maxWaitTime = std::chrono::milliseconds{reflected.maxWaitTimeMs.value()},
        .requestTimeout = std::chrono::seconds{reflected.requestTimeoutSeconds.value()},
        .apiKeyEnvVar = reflected.apiKeyEnvVar,
        .backend = std::move(reflected.backend).value()};

    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the
    /// reflected form. Schema's user-declared destructor suppresses its implicit move ctor,
    /// so the schema fields are passed by value.
    SemanticModelSchema schema{.inputs = reflected.inputs.value(), .outputs = reflected.outputs.value()};
    return RegisteredSemanticModel{std::move(reflected.name).value(), std::move(config), std::move(schema)};
}

}
