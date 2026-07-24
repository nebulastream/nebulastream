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
#include <DataTypes/UnboundField.hpp>
#include <ErrorHandling.hpp>
#include <Inference.hpp>
#include <Model.hpp>

namespace NES
{

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

    /// The runtime is f32-only and the physical operator writes/reads one f32
    /// slot per non-VARSIZED field. So declared fields must be FLOAT32, except
    /// for the bulk-byte VARSIZED escape hatch — a single field that mirrors
    /// the whole tensor verbatim. Validating this here makes
    /// `model ↔ modelSchema` compatibility an invariant downstream.
    const auto validateSide = [&](const ModelFieldList& fields, const std::vector<size_t>& tensorShape, std::string_view role)
    {
        bool hasVarsized = false;
        for (const auto& field : fields)
        {
            const auto type = field.getDataType().type;
            if (type != DataType::Type::FLOAT32 && type != DataType::Type::VARSIZED)
            {
                throw NES::CannotLoadModel(
                    "Model '{}' {} field '{}': type must be FLOAT32 or VARSIZED", name, role, field.getFullyQualifiedName());
            }
            if (type == DataType::Type::VARSIZED)
            {
                hasVarsized = true;
            }
        }
        if (hasVarsized && fields.size() != 1)
        {
            throw NES::CannotLoadModel("Model '{}' {}: VARSIZED requires exactly one {} field but got {}", name, role, role, fields.size());
        }
        if (!hasVarsized)
        {
            const size_t elementCount = std::accumulate(tensorShape.begin(), tensorShape.end(), size_t{1}, std::multiplies<>());
            if (fields.size() != elementCount)
            {
                throw NES::CannotLoadModel(
                    "Model '{}' {}: declared {} field(s) but tensor has {} element(s)", name, role, fields.size(), elementCount);
            }
        }
    };
    validateSide(schema.inputs, imported->getInputShape(), "input");
    validateSide(schema.outputs, imported->getOutputShape(), "output");

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
