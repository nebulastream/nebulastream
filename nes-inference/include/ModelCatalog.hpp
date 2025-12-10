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
#include <DataTypes/DataTypeProvider.hpp>
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
};

class ModelCatalog
{
    mutable std::unordered_map<std::string, Model> catalogImpl;
    std::unordered_map<std::string, ModelDescriptor> registeredModels;

public:
    void registerModel(const ModelDescriptor& model);
    Model load(const std::string Model) const;
    size_t getTypeSizeC(DataType::Type dtype) const
    {
        size_t size = 0;
        switch (dtype)
        {
            case DataType::Type::UINT8: size = sizeof(uint8_t); break;
            case DataType::Type::UINT16: size = sizeof(uint16_t); break;
            case DataType::Type::UINT32: size = sizeof(uint32_t); break;
            case DataType::Type::UINT64: size = sizeof(uint64_t); break;
            case DataType::Type::INT8: size = sizeof(int8_t); break;
            case DataType::Type::INT16: size = sizeof(int16_t); break;
            case DataType::Type::INT32: size = sizeof(int32_t); break;
            case DataType::Type::INT64: size = sizeof(int64_t); break;
            case DataType::Type::FLOAT32: size = sizeof(float); break;
            case DataType::Type::FLOAT64: size = sizeof(double); break;

            case DataType::Type::BOOLEAN:
            case DataType::Type::CHAR:
            case DataType::Type::UNDEFINED:
            case DataType::Type::VARSIZED:
            case DataType::Type::VARSIZED_POINTER_REP:
                throw std::runtime_error("ModelCatalog: Unsupported data type");
        }
        return size;
    }
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
            auto second = it->second;
            result->inputs = it->second.inputs;
            result->outputs = it->second.outputs
                | std::views::transform([](const auto& schemaField)
                                        { return std::pair<std::string, DataType>{schemaField.name, schemaField.dataType}; })
                | std::ranges::to<std::vector>();

            if (result->inputDtype.type == DataType::Type::UNDEFINED)
            {
                result->inputDtype = DataTypeProvider::provideDataType(result->inputs.at(0).type);
            }

            if (result->outputDtype.type == DataType::Type::UNDEFINED)
            {
                result->outputDtype = DataTypeProvider::provideDataType(result->outputs.at(0).second.type);
            }

            result->inputDims = result->inputShape.size();
            result->inputSizeInBytes = getTypeSizeC(result->inputDtype.type) *
                std::accumulate(result->inputShape.begin(), result->inputShape.end(), 1, std::multiplies<int>());

            result->outputDims = result->outputShape.size();
            result->outputSizeInBytes = getTypeSizeC(result->outputDtype.type) *
                std::accumulate(result->outputShape.begin(), result->outputShape.end(), 1, std::multiplies<int>());

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
