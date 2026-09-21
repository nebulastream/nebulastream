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

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <Util/URI.hpp>

namespace NES
{

namespace detail
{
struct ReflectedSemanticStep
{
    uint8_t kind = 0;
    std::string prompt;
    std::vector<std::string> outputValues;
    std::string defaultValue;
};

struct
    ReflectedRegisteredSemanticModel /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding optionals of vector-backed types trip the check; no real escape
{
    std::optional<std::string> name;
    std::optional<std::string> baseUrl;
    std::optional<std::string> model;
    std::optional<std::string> apiKeyEnv;
    std::optional<std::vector<ReflectedSemanticStep>> steps;
    std::optional<size_t> batchSize;
    std::optional<size_t> maxConcurrency;
    std::optional<size_t> maxRetries;
    std::optional<int64_t> requestTimeoutSeconds;
    std::optional<int64_t> connectTimeoutMillis;
    std::optional<SemanticModelFieldList> inputs;
    std::optional<SemanticModelFieldList> outputs;
};
}

Reflected Reflector<RegisteredSemanticModel>::operator()(const RegisteredSemanticModel& model, const ReflectionContext& context) const
{
    std::vector<detail::ReflectedSemanticStep> steps;
    steps.reserve(model.getConfig().steps.size());
    for (const auto& step : model.getConfig().steps)
    {
        steps.push_back(detail::ReflectedSemanticStep{
            .kind = static_cast<uint8_t>(step.kind), .prompt = step.prompt, .outputValues = step.outputValues, .defaultValue = step.defaultValue});
    }
    return context.reflect(detail::ReflectedRegisteredSemanticModel{
        .name = std::make_optional(model.getName()),
        .baseUrl = std::make_optional(model.getConfig().baseUrl),
        .model = std::make_optional(model.getConfig().model),
        .apiKeyEnv = model.getConfig().apiKeyEnv,
        .steps = std::make_optional(std::move(steps)),
        .batchSize = std::make_optional(model.getConfig().batchSize),
        .maxConcurrency = std::make_optional(model.getConfig().maxConcurrency),
        .maxRetries = std::make_optional(model.getConfig().maxRetries),
        .requestTimeoutSeconds = std::make_optional(model.getConfig().requestTimeout.count()),
        .connectTimeoutMillis = std::make_optional(model.getConfig().connectTimeout.count()),
        .inputs = std::make_optional(model.getSchema().inputs),
        .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredSemanticModel Unreflector<RegisteredSemanticModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedRegisteredSemanticModel>(rfl);
    if (!reflected.name.has_value() || !reflected.baseUrl.has_value() || !reflected.model.has_value() || !reflected.steps.has_value()
        || !reflected.batchSize.has_value() || !reflected.maxConcurrency.has_value() || !reflected.maxRetries.has_value()
        || !reflected.requestTimeoutSeconds.has_value() || !reflected.connectTimeoutMillis.has_value() || !reflected.inputs.has_value()
        || !reflected.outputs.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize RegisteredSemanticModel");
    }
    std::vector<SemanticStep> steps;
    steps.reserve(reflected.steps->size());
    for (auto& step : reflected.steps.value())
    {
        steps.push_back(SemanticStep{
            .kind = static_cast<SemanticStep::Kind>(step.kind),
            .prompt = std::move(step.prompt),
            .outputValues = std::move(step.outputValues),
            .defaultValue = std::move(step.defaultValue)});
    }
    SemanticModelConfig config{
        .baseUrl = std::move(reflected.baseUrl).value(),
        .model = std::move(reflected.model).value(),
        .apiKeyEnv = std::move(reflected.apiKeyEnv),
        .steps = std::move(steps),
        .batchSize = reflected.batchSize.value(),
        .maxConcurrency = reflected.maxConcurrency.value(),
        .maxRetries = reflected.maxRetries.value(),
        .requestTimeout = std::chrono::seconds(reflected.requestTimeoutSeconds.value()),
        .connectTimeout = std::chrono::milliseconds(reflected.connectTimeoutMillis.value())};
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    /// Schema's user-declared destructor suppresses its implicit move ctor; std::move on the field initializers
    /// would just rebind to copy-from-const-ref. Pass by value.
    SemanticModelSchema schema{.inputs = reflected.inputs.value(), .outputs = reflected.outputs.value()};
    return RegisteredSemanticModel{std::move(reflected.name).value(), std::move(config), std::move(schema)};
}

void SemanticModelCatalog::registerModel(std::string name, SemanticModelConfig config, SemanticModelSchema schema)
{
    /// Endpoint reachability is deliberately not checked here — DDL must not fail on a cold
    /// Ollama. Only the shape of the config is validated.
    if (entries.contains(name))
    {
        throw NES::ModelAlreadyExists(name);
    }
    if (!URI::tryParse(config.baseUrl))
    {
        throw NES::CannotLoadModel("Semantic model '{}': BASE_URL '{}' is not a valid URL", name, config.baseUrl);
    }
    if (config.model.empty())
    {
        throw NES::CannotLoadModel("Semantic model '{}': MODEL must not be empty", name);
    }
    if (config.steps.size() != schema.outputs.size())
    {
        throw NES::CannotLoadModel(
            "Semantic model '{}': {} step(s) declared but OUTPUT has {} field(s) — step i pairs with output i",
            name,
            config.steps.size(),
            schema.outputs.size());
    }
    for (const auto& step : config.steps)
    {
        if (step.prompt.empty())
        {
            throw NES::CannotLoadModel("Semantic model '{}': PROMPT must not be empty", name);
        }
    }
    if (config.apiKeyEnv.has_value() && std::getenv(config.apiKeyEnv->c_str()) == nullptr)
    {
        throw NES::CannotLoadModel(
            "Semantic model '{}': API_KEY_ENV names environment variable '{}' which is not set", name, *config.apiKeyEnv);
    }
    if (config.batchSize > 1 || config.maxConcurrency > 1)
    {
        throw NES::InvalidConfigParameter(
            "Semantic model '{}': BATCH_SIZE > 1 and MAX_CONCURRENCY > 1 require the asynchronous execution path (Phase 2)", name);
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
    throw UnknownModelName("Semantic model '{}' was never registered", modelName);
}

}
