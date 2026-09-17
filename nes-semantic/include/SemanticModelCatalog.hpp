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

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <SemanticModelConfig.hpp>

namespace NES
{

class SemanticModelCatalog;

/// Catalog-side field schema for a semantic model — ordered list of name + DataType pairs,
/// mirroring `ModelFieldList` (nes-inference/include/ModelCatalog.hpp). Kept as its own type
/// rather than reused from nes-inference: SEM_MAP's INPUT/OUTPUT declaration has no relation to
/// the tensor-backed model catalog and shouldn't couple to it.
using SemanticModelFieldList = Schema<UnqualifiedUnboundField, Ordered>;

/// User-declared INPUT and OUTPUT field schemas of a `CREATE SEMANTIC MODEL`. Populated once the
/// logical layer (plan §M2) resolves them; empty for a catalog entry registered standalone.
struct
    SemanticModelSchema /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding Schema (vector) trip the check; no real escape
{
    SemanticModelFieldList inputs;
    SemanticModelFieldList outputs;

    bool operator==(const SemanticModelSchema&) const = default;
};

/// A catalog entry: the user-given name, the `SET (...)` options, and the declared INPUT/OUTPUT
/// schema.
///
/// Constructible only through `SemanticModelCatalog::registerModel` (which validates). Callers
/// must route through it — there is no public constructor.
class RegisteredSemanticModel
{
    std::string name;
    SemanticModelConfig config;
    SemanticModelSchema schema;

    RegisteredSemanticModel(std::string name, SemanticModelConfig config, SemanticModelSchema schema)
        : name(std::move(name)), config(std::move(config)), schema(std::move(schema))
    {
    }

    friend class NES::SemanticModelCatalog;

public:
    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const SemanticModelConfig& getConfig() const { return config; }

    [[nodiscard]] const SemanticModelSchema& getSchema() const { return schema; }

    bool operator==(const RegisteredSemanticModel&) const = default;
};

/// Manages `CREATE SEMANTIC MODEL` registration. Validates the config at registration time
/// (BASE_URL parses as a URL, MODEL/PROMPT non-empty, API_KEY_ENV names a set env var) but never
/// contacts the endpoint — see plan §M3. Not thread-safe — concurrent access requires external
/// synchronization.
class SemanticModelCatalog
{
    std::unordered_map<std::string, RegisteredSemanticModel> entries;

public:
    void registerModel(std::string name, SemanticModelConfig config, SemanticModelSchema schema = {});
    void removeModel(const std::string& modelName);
    [[nodiscard]] bool hasModel(const std::string& modelName) const;
    [[nodiscard]] std::vector<std::string> getModelNames() const;
    [[nodiscard]] std::vector<RegisteredSemanticModel> getRegisteredModels() const;
    [[nodiscard]] RegisteredSemanticModel load(const std::string& modelName) const;
};

}
