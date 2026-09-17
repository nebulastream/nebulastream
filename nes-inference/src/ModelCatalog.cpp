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

#include <ModelCatalog.hpp>

#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <Model.hpp>

namespace NES
{

void ModelCatalog::registerModel(std::string name, std::filesystem::path path, ModelSchema schema)
{
    auto registered = RegisteredModel::create(name, std::move(path), std::move(schema));
    entries.insert_or_assign(std::move(name), std::move(registered));
}

void ModelCatalog::removeModel(const std::string& modelName)
{
    entries.erase(modelName);
}

bool ModelCatalog::hasModel(const std::string& modelName) const
{
    return entries.contains(modelName);
}

std::vector<std::string> ModelCatalog::getModelNames() const
{
    std::vector<std::string> names;
    names.reserve(entries.size());
    for (const auto& [name, _] : entries)
    {
        names.push_back(name);
    }
    return names;
}

std::vector<RegisteredModel> ModelCatalog::getRegisteredModels() const
{
    std::vector<RegisteredModel> models;
    models.reserve(entries.size());
    for (const auto& [_, model] : entries)
    {
        models.push_back(model);
    }
    return models;
}

RegisteredModel ModelCatalog::load(const std::string& modelName) const
{
    if (auto it = entries.find(modelName); it != entries.end())
    {
        return it->second;
    }
    throw UnknownModelName("Model '{}' was never registered", modelName);
}

}
