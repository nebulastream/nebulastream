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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

class SemanticModelCatalog;

/// How the values of the declared input fields are serialized into the prompt payload.
enum class PayloadFormat : uint8_t
{
    /// Faithful to the Python reference: every input value stringified and joined by a
    /// single space, without field names. Lossy, but it is what the published numbers
    /// were measured with, so it stays the default.
    SPACE_JOINED,
    /// One JSON object per row carrying the real field names.
    JSON_OBJECT,
};

/// One semantic step. SEM_MAP always registers exactly one. The list exists from the
/// start so that operator fusion — merging an adjacent MAP and FILTER into a single
/// prompt — does not later force a change to the catalog entry and its wire format.
struct SemanticStep
{
    enum class Kind : uint8_t
    {
        MAP,
        FILTER,
    };

    Kind kind = Kind::MAP;
    std::string prompt;
    std::string outputColumn;
    /// Closed set of admissible answers. Empty means free text, in which case the
    /// model's answer is taken verbatim.
    std::vector<std::string> outputValues;
    /// Written whenever the model omits this row or the response cannot be parsed.
    std::string defaultValue;

    bool operator==(const SemanticStep&) const = default;
};

/// Everything needed to talk to the remote model. Catalog-side metadata declared
/// alongside the model in `CREATE SEMANTIC MODEL`; none of it is derived from the
/// endpoint itself.
struct SemanticModelConfig
{
    /// OpenAI-compatible base URL, e.g. http://localhost:8000/v1
    std::string endpoint;
    /// Model identifier passed through to the endpoint, e.g. meta-llama/Llama-3.3-70B-Instruct
    std::string modelName;
    /// Optional free-text description of the dataset, prepended to the prompt.
    std::string datasetPrompt;
    std::vector<SemanticStep> steps;
    PayloadFormat payloadFormat = PayloadFormat::SPACE_JOINED;
    /// Records per request. 1 reproduces the Python default.
    size_t batchSize = 1;
    /// Requests in flight per model.
    size_t maxConcurrency = 10;
    size_t maxRetries = 2;
    /// How long a partial batch waits before it is flushed anyway.
    std::chrono::milliseconds maxWaitTime{1000};
    std::chrono::seconds requestTimeout{600};
    /// Name of the environment variable holding the API key — never the key itself.
    /// The catalog entry travels from the coordinator to the workers, so a secret
    /// stored here would travel with it. Resolved worker-locally during lowering.
    std::optional<std::string> apiKeyEnvVar;
    /// Selects the backend implementation. "http" talks to `endpoint`; "mock" is the
    /// deterministic, network-free backend the system tests run against.
    std::string backend = "http";

    bool operator==(const SemanticModelConfig&) const = default;
};

/// Catalog-side field schema — an ordered list of name + DataType pairs. Ordered
/// because the prompt payload serializes input values positionally.
using SemanticFieldList = Schema<UnqualifiedUnboundField, Ordered>;

/// User-declared input and output field schemas, from the INPUT(...) and OUTPUT(...)
/// clauses of `CREATE SEMANTIC MODEL`.
struct
    SemanticModelSchema /// NOLINT(bugprone-exception-escape) defaulted special members on a struct holding Schema (vector) trip the check; no real escape
{
    SemanticFieldList inputs;
    SemanticFieldList outputs;

    bool operator==(const SemanticModelSchema&) const = default;
};

/// A catalog entry: the user-given name together with the validated configuration
/// and field schema.
///
/// Constructible only through `SemanticModelCatalog::registerModel` (which validates)
/// or through reflection (which trusts the coordinator-side checks).
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
    friend struct Reflector<RegisteredSemanticModel>;
    friend struct Unreflector<RegisteredSemanticModel>;

public:
    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const SemanticModelConfig& getConfig() const { return config; }

    [[nodiscard]] const SemanticModelSchema& getSchema() const { return schema; }

    bool operator==(const RegisteredSemanticModel&) const = default;
};

template <>
struct Reflector<RegisteredSemanticModel>
{
    Reflected operator()(const RegisteredSemanticModel& model, const ReflectionContext& context) const;
};

template <>
struct Unreflector<RegisteredSemanticModel>
{
    RegisteredSemanticModel operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

/// Manages registration of semantic models. Unlike `ModelCatalog` there is nothing to
/// import or compile — an entry is pure metadata, so registration only validates.
/// Not thread-safe; concurrent access requires external synchronization.
class SemanticModelCatalog
{
    std::unordered_map<std::string, RegisteredSemanticModel> entries;

public:
    void registerModel(std::string name, SemanticModelConfig config, SemanticModelSchema schema);
    void removeModel(const std::string& modelName);
    [[nodiscard]] bool hasModel(const std::string& modelName) const;
    [[nodiscard]] std::vector<std::string> getModelNames() const;
    [[nodiscard]] std::vector<RegisteredSemanticModel> getRegisteredModels() const;
    [[nodiscard]] RegisteredSemanticModel load(const std::string& modelName) const;
};

}
