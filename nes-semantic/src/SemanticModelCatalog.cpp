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
struct
    ReflectedRegisteredSemanticModel /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding optionals of vector-backed types trip the check; no real escape
{
    std::optional<std::string> name;
    std::optional<std::string> baseUrl;
    std::optional<std::string> model;
    std::optional<std::string> prompt;
    std::optional<std::string> apiKeyEnv;
    std::optional<std::vector<std::string>> outputValues;
    std::optional<SemanticModelFieldList> inputs;
    std::optional<SemanticModelFieldList> outputs;
};
}

Reflected Reflector<RegisteredSemanticModel>::operator()(const RegisteredSemanticModel& model, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedRegisteredSemanticModel{
        .name = std::make_optional(model.getName()),
        .baseUrl = std::make_optional(model.getConfig().baseUrl),
        .model = std::make_optional(model.getConfig().model),
        .prompt = std::make_optional(model.getConfig().prompt),
        .apiKeyEnv = model.getConfig().apiKeyEnv,
        .outputValues = model.getConfig().outputValues,
        .inputs = std::make_optional(model.getSchema().inputs),
        .outputs = std::make_optional(model.getSchema().outputs)});
}

RegisteredSemanticModel Unreflector<RegisteredSemanticModel>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedRegisteredSemanticModel>(rfl);
    if (!reflected.name.has_value() || !reflected.baseUrl.has_value() || !reflected.model.has_value() || !reflected.prompt.has_value()
        || !reflected.inputs.has_value() || !reflected.outputs.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize RegisteredSemanticModel");
    }
    SemanticModelConfig config{
        .baseUrl = std::move(reflected.baseUrl).value(),
        .model = std::move(reflected.model).value(),
        .prompt = std::move(reflected.prompt).value(),
        .apiKeyEnv = std::move(reflected.apiKeyEnv),
        .outputValues = std::move(reflected.outputValues)};
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
    if (!URI::tryParse(config.baseUrl))
    {
        throw NES::CannotLoadModel("Semantic model '{}': BASE_URL '{}' is not a valid URL", name, config.baseUrl);
    }
    if (config.model.empty())
    {
        throw NES::CannotLoadModel("Semantic model '{}': MODEL must not be empty", name);
    }
    if (config.prompt.empty())
    {
        throw NES::CannotLoadModel("Semantic model '{}': PROMPT must not be empty", name);
    }
    if (config.apiKeyEnv.has_value() && std::getenv(config.apiKeyEnv->c_str()) == nullptr)
    {
        throw NES::CannotLoadModel(
            "Semantic model '{}': API_KEY_ENV names environment variable '{}' which is not set", name, *config.apiKeyEnv);
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
