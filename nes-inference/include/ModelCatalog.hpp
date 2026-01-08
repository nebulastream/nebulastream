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
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <folly/Synchronized.h>
#include <magic_enum/magic_enum.hpp>

namespace NES::Nebuli::Inference
{

struct ModelDescriptor
{
    std::string name;
    std::filesystem::path path;
    std::vector<DataType> inputs;
    Schema outputs;
};

class ModelCatalog
{
    mutable std::unordered_map<std::string, Model> catalogImpl;
    folly::Synchronized<std::unordered_map<std::string, ModelDescriptor>> registeredModels;

public:
    void registerModel(const ModelDescriptor& model);
    Model load(const std::string Model) const;

    std::optional<ModelDescriptor> addModelDescriptor(std::string name, std::string path, std::vector<DataType> inputs, Schema outputs);
    std::optional<ModelDescriptor> getModelDescriptor(const std::string name);
    std::vector<ModelDescriptor> getAllModelDescriptors() const;
    bool removeModelDescriptor(const ModelDescriptor& modelDescriptor);
    size_t getTypeSizeC(DataType::Type dtype) const;
};

inline void ModelCatalog::registerModel(const ModelDescriptor& model)
{
    auto modelsLock = registeredModels.wlock();
    modelsLock->emplace(model.name, model);
}

inline Model ModelCatalog::load(const std::string modelName) const
{
    if (auto it = catalogImpl.find(modelName); it != catalogImpl.end())
    {
        return it->second;
    }

    auto modelsLock = registeredModels.rlock();
    if (auto it = modelsLock->find(modelName); it != modelsLock->end())
    {
        auto result = Inference::load(it->second.path, {});
        if (result)
        {
            auto second = it->second;
            result->inputs = it->second.inputs;
            result->outputs = it->second.outputs
                | std::views::transform([](const auto& schemaField)
                                        { return std::pair<std::string, DataType>{schemaField.name, schemaField.dataType}; })
                | std::ranges::to<std::vector>();

            /// if we didn't manage to parse the data type, we rely on the user-provided types
            /// we take the data type of the first field, because it must be the same for all the fields
            if (result->inputDtype.type == DataType::Type::UNDEFINED
                || (result->inputs.at(0).type != DataType::Type::VARSIZED && result->inputDtype.type != result->inputs.at(0).type))
            {
                result->inputDtype = DataTypeProvider::provideDataType(result->inputs.at(0).type);
            }

            if (result->outputDtype.type == DataType::Type::UNDEFINED
                || (result->outputs.at(0).second.type != DataType::Type::VARSIZED
                    && result->outputDtype.type != result->outputs.at(0).second.type))
            {
                result->outputDtype = DataTypeProvider::provideDataType(result->outputs.at(0).second.type);
            }

            result->inputDims = result->inputShape.size();
            result->inputSizeInBytes = getTypeSizeC(result->inputDtype.type)
                * std::accumulate(result->inputShape.begin(), result->inputShape.end(), 1, std::multiplies<int>());

            result->outputDims = result->outputShape.size();
            result->outputSizeInBytes = getTypeSizeC(result->outputDtype.type)
                * std::accumulate(result->outputShape.begin(), result->outputShape.end(), 1, std::multiplies<int>());

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

inline std::optional<ModelDescriptor>
ModelCatalog::addModelDescriptor(std::string name, std::string path, std::vector<DataType> inputs, Schema outputs)
{
    auto model = ModelDescriptor{std::move(name), std::move(path), std::move(inputs), std::move(outputs)};
    auto modelsLock = registeredModels.wlock();
    modelsLock->emplace(std::move(name), model);
    return model;
}

inline std::optional<ModelDescriptor> ModelCatalog::getModelDescriptor(const std::string name)
{
    auto modelsLock = registeredModels.rlock();
    const auto modelDescriptorOpt = modelsLock->find(std::string{name});
    if (modelDescriptorOpt == modelsLock->end())
    {
        return std::nullopt;
    }
    return modelDescriptorOpt->second;
}

inline std::vector<ModelDescriptor> ModelCatalog::getAllModelDescriptors() const
{
    const auto modelsLock = registeredModels.rlock();
    return *modelsLock | std::ranges::views::transform([](const auto& modelDescriptor) { return modelDescriptor.second; })
        | std::ranges::to<std::vector>();
}

inline bool ModelCatalog::removeModelDescriptor(const ModelDescriptor& modelDescriptor)
{
    const auto modelsLock = registeredModels.wlock();
    return modelsLock->erase(modelDescriptor.name) == 1;
}

inline size_t ModelCatalog::getTypeSizeC(DataType::Type dtype) const
{
    size_t size = 0;
    switch (dtype)
    {
        case DataType::Type::UINT8:
            size = sizeof(uint8_t);
            break;
        case DataType::Type::UINT16:
            size = sizeof(uint16_t);
            break;
        case DataType::Type::UINT32:
            size = sizeof(uint32_t);
            break;
        case DataType::Type::UINT64:
            size = sizeof(uint64_t);
            break;
        case DataType::Type::INT8:
            size = sizeof(int8_t);
            break;
        case DataType::Type::INT16:
            size = sizeof(int16_t);
            break;
        case DataType::Type::INT32:
            size = sizeof(int32_t);
            break;
        case DataType::Type::INT64:
            size = sizeof(int64_t);
            break;
        case DataType::Type::FLOAT32:
            size = sizeof(float);
            break;
        case DataType::Type::FLOAT64:
            size = sizeof(double);
            break;

        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(dtype));
    }
    return size;
}

}
