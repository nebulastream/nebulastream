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

#include <filesystem>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Util/Reflection.hpp>
#include <Model.hpp>

namespace NES
{

class ModelCatalog;

/// User-declared input and output field schemas of a model. Not a property of
/// the MLIR/bytecode — it's catalog-side metadata, declared alongside the model
/// in `CREATE MODEL` and passed through to the logical operator for schema
/// inference.
struct
    ModelSchema /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding Schema (vector) trip the check; no real escape
{
    Schema inputs;
    Schema outputs;

    bool operator==(const ModelSchema&) const = default;
};

/// A catalog entry: the user-given name and source path together with the
/// imported model and the validated schema.
///
/// Constructible only through `ModelCatalog::registerModel` (which validates)
/// or through reflection (which trusts the coordinator-side checks). Callers
/// must route through those paths — there is no public constructor.
class RegisteredModel
{
    std::string name;
    std::filesystem::path path;
    ImportedModel imported;
    ModelSchema schema;

    RegisteredModel(std::string name, std::filesystem::path path, ImportedModel importedModel, ModelSchema modelSchema)
        : name(std::move(name)), path(std::move(path)), imported(std::move(importedModel)), schema(std::move(modelSchema))
    {
    }

    friend class NES::ModelCatalog;
    friend struct Reflector<RegisteredModel>;
    friend struct Unreflector<RegisteredModel>;

public:
    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const std::filesystem::path& getPath() const { return path; }

    [[nodiscard]] const ImportedModel& getImported() const { return imported; }

    [[nodiscard]] const ModelSchema& getSchema() const { return schema; }

    bool operator==(const RegisteredModel&) const = default;
};

template <>
struct Reflector<RegisteredModel>
{
    Reflected operator()(const RegisteredModel& model) const;
};

template <>
struct Unreflector<RegisteredModel>
{
    RegisteredModel operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

/// Manages model registration and stores imported model bodies keyed by name.
/// Compilation to executable bytecode is deferred until worker-side lowering.
/// Not thread-safe — concurrent access requires external synchronization.
class ModelCatalog
{
    std::unordered_map<std::string, RegisteredModel> entries;

public:
    void registerModel(std::string name, std::filesystem::path path, ModelSchema schema);
    void removeModel(const std::string& modelName);
    [[nodiscard]] bool hasModel(const std::string& modelName) const;
    [[nodiscard]] std::vector<std::string> getModelNames() const;
    [[nodiscard]] std::vector<RegisteredModel> getRegisteredModels() const;
    [[nodiscard]] RegisteredModel load(const std::string& modelName) const;
};

}
