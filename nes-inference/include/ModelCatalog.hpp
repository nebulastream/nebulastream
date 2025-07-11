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

#include <DataTypes/Schema.hpp>
#include <ranges>
#include <unordered_map>

namespace NES::Nebuli::Inference
{

struct ModelDescriptor
{
    std::string name;
    std::filesystem::path path;
    std::vector<DataType> inputs;
    Schema outputs;
    size_t batchSize;
};

class ModelCatalog
{
    mutable std::unordered_map<std::string, Model> catalogImpl;
    std::unordered_map<std::string, ModelDescriptor> registeredModels;

public:
    void registerModel(const ModelDescriptor& model);
    Model load(const std::string Model) const;
};

inline void ModelCatalog::registerModel(const ModelDescriptor& model)
{
    registeredModels.emplace(model.name, model);
}

inline Model ModelCatalog::load(const std::string modelName) const
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
            result->inputs = it->second.inputs;
            result->outputs = it->second.outputs
                | std::views::transform([](const auto& schemaField)
                                        { return std::pair<std::string, DataType>{schemaField.name, schemaField.dataType}; })
                | std::ranges::to<std::vector>();

            if (result->inputShape[0] == 0 && result->outputShape[0] == 0)
            {
                result->inputShape[0] = it->second.batchSize;
                result->outputShape[0] = it->second.batchSize;
                
                result->inputDims = result->inputShape.size();
                result->inputSizeInBytes = 4 * std::accumulate(result->inputShape.begin(), result->inputShape.end(), 1, std::multiplies<int>());

                result->outputDims = result->outputShape.size();
                result->outputSizeInBytes = 4 * std::accumulate(result->outputShape.begin(), result->outputShape.end(), 1, std::multiplies<int>());
            }
            else
            {
                NES_WARNING("The model has a fixed batch size of {}. The provided batch size of {} will be ignored. "
                    "Save the model with the variable first dimension of the input tensor to use the provided batch size.",
                    result->inputShape[0], it->second.batchSize);

                result->inputDims = result->inputShape.size();
                result->inputSizeInBytes = 4 * std::accumulate(result->inputShape.begin(), result->inputShape.end(), 1, std::multiplies<int>());

                result->outputDims = result->outputShape.size();
                result->outputSizeInBytes = 4 * std::accumulate(result->outputShape.begin(), result->outputShape.end(), 1, std::multiplies<int>());
            }

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
