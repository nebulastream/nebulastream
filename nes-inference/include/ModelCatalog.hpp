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

#pragma once
#include "ModelLoader.hpp"

#include <ranges>
#include <unordered_map>

namespace NES::Inference
{

/// Description of a registered model before compilation
struct RegisteredModel
{
    std::string name;
    std::filesystem::path path;
    std::vector<std::shared_ptr<NES::DataType>> inputs;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> outputs;
};

class ModelCatalog
{
    mutable std::unordered_map<std::string, Model> catalogImpl;
    std::unordered_map<std::string, RegisteredModel> registeredModels;

public:
    void registerModel(const RegisteredModel& model);
    void removeModel(const std::string& modelName);
    bool hasModel(const std::string& modelName) const;
    std::vector<std::string> getModelNames() const;
    Model load(const std::string& modelName) const;
};

inline void ModelCatalog::registerModel(const RegisteredModel& model)
{
    registeredModels.emplace(model.name, model);
}

inline void ModelCatalog::removeModel(const std::string& modelName)
{
    registeredModels.erase(modelName);
    catalogImpl.erase(modelName);
}

inline bool ModelCatalog::hasModel(const std::string& modelName) const
{
    return registeredModels.contains(modelName);
}

inline std::vector<std::string> ModelCatalog::getModelNames() const
{
    std::vector<std::string> names;
    for (const auto& [name, _] : registeredModels)
    {
        names.push_back(name);
    }
    return names;
}

inline Model ModelCatalog::load(const std::string& modelName) const
{
    if (auto it = catalogImpl.find(modelName); it != catalogImpl.end())
    {
        return it->second;
    }

    if (auto it = registeredModels.find(modelName); it != registeredModels.end())
    {
        auto result = Inference::load(it->second.path, {});
        if (result)
        {
            result->setInputs(it->second.inputs);
            result->setOutputs(it->second.outputs);

            catalogImpl.emplace(modelName, *result);
            return *result;
        }
        throw std::runtime_error(fmt::format("Failed to load model '{}': {}", modelName, result.error().message));
    }
    else
    {
        throw std::runtime_error(fmt::format("Model '{}' was never registered", modelName));
    }
}

}
