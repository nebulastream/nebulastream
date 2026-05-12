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

#include <cstddef>
#include <filesystem>
#include <functional>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <Inference.hpp>

namespace NES
{

namespace
{

DataType::Type queryDataTypeForTensorElementType(TensorElementType type)
{
    switch (type)
    {
        case TensorElementType::FLOAT32:
            return DataType::Type::FLOAT32;
        case TensorElementType::UINT8:
            return DataType::Type::UINT8;
        case TensorElementType::INT64:
            return DataType::Type::INT64;
    }
    std::unreachable();
}

}

void ModelCatalog::registerModel(std::string name, std::filesystem::path path, ModelSchema schema)
{
    if (!std::filesystem::exists(path))
    {
        throw NES::InvalidStatement("Model path does not exist: {}", path);
    }

    /// Coordinator-side: only import the model — signature is scraped during
    /// import. The compile step runs later on the worker, during lowering.
    auto imported = importModel(path);
    if (!imported)
    {
        throw NES::CannotLoadModel("Failed to import model '{}': {}", name, imported.error().message);
    }

    /// Non-VARSIZED model declarations must match the tensor element type so
    /// the physical operator can write/read one native element per field.
    /// VARSIZED remains the bulk-byte escape hatch that mirrors the whole
    /// tensor verbatim. Validating this here makes
    /// `model ↔ modelSchema` compatibility an invariant downstream.
    const auto validateSide =
        [&](const Schema& fields, const std::vector<size_t>& tensorShape, TensorElementType tensorElementType, std::string_view role)
    {
        const auto expectedFieldType = queryDataTypeForTensorElementType(tensorElementType);
        const DataType expectedDataType{expectedFieldType, DataType::NULLABLE::NOT_NULLABLE};
        bool hasVarsized = false;
        for (const auto& field : fields.getFields())
        {
            const auto type = field.dataType.type;
            if (type != expectedFieldType && type != DataType::Type::VARSIZED)
            {
                throw NES::CannotLoadModel(
                    "Model '{}' {} field '{}': type must be {} or VARSIZED", name, role, field.name, expectedDataType);
            }
            if (type == DataType::Type::VARSIZED)
            {
                hasVarsized = true;
            }
        }
        if (hasVarsized && fields.getNumberOfFields() != 1)
        {
            throw NES::CannotLoadModel(
                "Model '{}' {}: VARSIZED requires exactly one {} field but got {}", name, role, role, fields.getNumberOfFields());
        }
        if (!hasVarsized)
        {
            const size_t elementCount = std::accumulate(tensorShape.begin(), tensorShape.end(), size_t{1}, std::multiplies<>());
            if (fields.getNumberOfFields() != elementCount)
            {
                throw NES::CannotLoadModel(
                    "Model '{}' {}: declared {} field(s) but tensor has {} element(s)",
                    name,
                    role,
                    fields.getNumberOfFields(),
                    elementCount);
            }
        }
    };
    validateSide(schema.inputs, imported->getInputShape(), imported->getInputElementType(), "input");
    validateSide(schema.outputs, imported->getOutputShape(), imported->getOutputElementType(), "output");

    auto registered = RegisteredModel{name, std::move(path), std::move(*imported), std::move(schema)};
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
