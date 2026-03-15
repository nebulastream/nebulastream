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
#include <ModelLoader.hpp>

#include <filesystem>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Inference
{

/// Description of a registered model before compilation
struct RegisteredModel
{
    std::string name;
    std::filesystem::path path;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> inputs;
    std::vector<std::pair<std::string, std::shared_ptr<NES::DataType>>> outputs;
};

/// Manages model registration and lazy-loading/caching of compiled models.
/// `catalogImpl` is mutable to allow caching compiled models in const `load()`.
/// Not thread-safe — concurrent access requires external synchronization.
class ModelCatalog
{
    mutable std::unordered_map<std::string, Model> catalogImpl;
    std::unordered_map<std::string, RegisteredModel> registeredModels;

public:
    void registerModel(const RegisteredModel& model);
    void removeModel(const std::string& modelName);
    bool hasModel(const std::string& modelName) const;
    std::vector<std::string> getModelNames() const;
    const RegisteredModel& getRegisteredModel(const std::string& modelName) const;
    Model load(const std::string& modelName) const;
};

inline void ModelCatalog::registerModel(const RegisteredModel& model)
{
    if (!std::filesystem::exists(model.path))
    {
        throw NES::InvalidStatement("Model path does not exist: {}", model.path);
    }
    catalogImpl.erase(model.name);
    registeredModels.insert_or_assign(model.name, model);
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

inline const RegisteredModel& ModelCatalog::getRegisteredModel(const std::string& modelName) const
{
    return registeredModels.at(modelName);
}

inline std::vector<std::string> ModelCatalog::getModelNames() const
{
    std::vector<std::string> names;
    names.reserve(registeredModels.size());
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
        const auto& reg = it->second;
        auto result = Inference::load(reg.path, {});
        if (result)
        {
            std::vector<NES::DataType> inputTypes;
            inputTypes.reserve(reg.inputs.size());
            for (const auto& [name, dt] : reg.inputs)
            {
                inputTypes.push_back(*dt);
            }
            result->setInputs(std::move(inputTypes));

            std::vector<std::pair<std::string, NES::DataType>> outputFields;
            outputFields.reserve(reg.outputs.size());
            for (const auto& [name, dt] : reg.outputs)
            {
                outputFields.emplace_back(name, *dt);
            }
            result->setOutputs(std::move(outputFields));

            catalogImpl.emplace(modelName, *result);
            return *result;
        }
        throw CannotLoadModel("Failed to load model '{}': {}", modelName, result.error().message);
    }
    throw UnknownModelName("Model '{}' was never registered", modelName);
}

}
