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

/// Where the model call is made. A semantic operator waits for a remote model, which under
/// SYNCHRONOUS execution happens on one of the engine's worker threads: a handful of concurrent
/// calls then occupy the whole pool and stall unrelated queries. ASYNCHRONOUS hands the operator
/// to the framework in `nes-async`, which runs it on threads of its own.
///
/// SYNCHRONOUS stays the default because it is the simpler deployment; a query that needs
/// throughput sets LLM.EXECUTION to 'ASYNC'. Both produce the same results, which is what makes
/// them comparable in a measurement.
enum class SemanticExecution : uint8_t
{
    SYNCHRONOUS,
    ASYNCHRONOUS,
};

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

/// One semantic step. `CREATE SEM_MODEL` always registers exactly one: a MAP step when the
/// model declares an OUTPUT clause, a FILTER step when it does not. Operator fusion
/// (`fuseSemanticModels`) concatenates the step lists of adjacent operators, so that one prompt
/// answers all of them.
struct SemanticStep
{
    enum class Kind : uint8_t
    {
        MAP,
        FILTER,
    };

    Kind kind = Kind::MAP;
    std::string prompt;
    /// A MAP step's OUTPUT field. A FILTER step produces no column; its verdict is requested under
    /// `__filter_<k>`, numbered by the codec, so the value stored here is informational only.
    std::string outputColumn;
    /// Closed set of admissible answers. Empty means free text, in which case the
    /// model's answer is taken verbatim. MAP only.
    std::vector<std::string> outputValues;
    /// Written whenever the model omits this row or the response cannot be parsed. MAP only: a
    /// FILTER step drops such a row instead.
    std::string defaultValue;

    bool operator==(const SemanticStep&) const = default;
};

/// Everything needed to talk to the remote model. Catalog-side metadata declared
/// alongside the model in `CREATE SEM_MODEL`; none of it is derived from the
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
    /// Whether the operator runs on a worker thread or on the asynchronous framework's own.
    SemanticExecution execution = SemanticExecution::SYNCHRONOUS;
    /// Whether results leave the operator in input order. Only the asynchronous path has a choice:
    /// it has several calls in flight and finishes them out of order. Ordering costs latency,
    /// because a finished record waits for the ones before it, so it is worth turning off where
    /// downstream does not care.
    bool preserveOrder = true;
    /// Whether this model may be fused with an adjacent semantic operator over the same input into a
    /// single prompt (`SemanticFusionRule`). Opt-in, like the reference's per-query `fusion=False`,
    /// so the measured unfused baselines stay reproducible.
    bool fusion = false;

    bool operator==(const SemanticModelConfig&) const = default;
};

/// Catalog-side field schema — an ordered list of name + DataType pairs. Ordered
/// because the prompt payload serializes input values positionally.
using SemanticFieldList = Schema<UnqualifiedUnboundField, Ordered>;

/// User-declared input and output field schemas, from the INPUT(...) and OUTPUT(...)
/// clauses of `CREATE SEM_MODEL`. `outputs` holds one field per MAP step, in step order, and is
/// empty for a filter model, which adds no column.
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
/// Constructible only through `SemanticModelCatalog::registerModel` (which validates),
/// through reflection (which trusts the coordinator-side checks) and through
/// `fuseSemanticModels` (which combines two entries that were each validated).
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
    friend RegisteredSemanticModel fuseSemanticModels(const RegisteredSemanticModel& first, const RegisteredSemanticModel& second);

public:
    [[nodiscard]] const std::string& getName() const { return name; }

    [[nodiscard]] const SemanticModelConfig& getConfig() const { return config; }

    [[nodiscard]] const SemanticModelSchema& getSchema() const { return schema; }

    /// Whether any step drops rows, i.e. whether the entry runs as SEM_FILTER rather than SEM_MAP.
    [[nodiscard]] bool hasFilterStep() const;

    bool operator==(const RegisteredSemanticModel&) const = default;
};

/// The model `Coordinator._apply_fusion` would build from two adjacent operators: `first` is the
/// upstream one. Steps are concatenated (upstream first) with FILTER steps renumbered
/// `__filter_0..n`, OUTPUT fields concatenated, and the INPUT fields and transport configuration
/// taken from `first`. The name is "<first>+<second>".
///
/// Whether the two may be fused at all — equal inputs, equal transport, `fusion` set on both — is
/// the caller's decision (`SemanticFusionRule`); this only throws `InvalidSemanticModel` when the
/// combined OUTPUT fields collide.
[[nodiscard]] RegisteredSemanticModel fuseSemanticModels(const RegisteredSemanticModel& first, const RegisteredSemanticModel& second);

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
