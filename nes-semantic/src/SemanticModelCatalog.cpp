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
#include <string>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <Util/URI.hpp>

namespace NES
{

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
