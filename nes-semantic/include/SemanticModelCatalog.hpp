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
#include <SemanticImage.hpp>

namespace NES
{

class SemanticModelCatalog;

/// Where the model call is made. Kept for the catalog's round trip and for `sameTransport` in fusion; on the async
/// framework every SEM_MAP and SEM_FILTER is translated into `SEM_CALL` and runs on the shared event loop, whatever is
/// declared here (`SemanticCallTranslationRule`).
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

/// SEM_SCAN and SEM_WINDOW: operators that keep state from row to row, per key (after llm_operator's `sem_scan.py` and
/// `sem_window.py`). The async framework runs the calls of one key one at a time and in stream order
/// (`AsyncFunction::orderingKey`); the state lives in `SemanticKeyedState`, shared by the stage's function instances.
struct SemanticKeyedConfig
{
    enum class Kind : uint8_t
    {
        /// A running accumulator per key: OUTPUT (state, confidence).
        SCAN,
        /// Semantic windows per key: OUTPUT (window_id, window_description, window_start, continuity_score, is_boundary).
        WINDOW,
    };

    Kind kind = Kind::SCAN;
    /// The field whose value is the key. Required for SCAN; empty makes the whole stream one key, WINDOW only.
    std::string keyBy;
    /// SCAN: the instruction for a key's first row. Without it, the first row's state is `initialState`, without a call.
    std::string initPrompt;
    std::string initialState;
    /// WINDOW: "summary" shows each window's recent examples, "pairwise" only its most recent row.
    std::string strategy = "summary";
    /// WINDOW: the continuity score a row needs to join its best window rather than open a new one.
    double threshold = 0.7;
    /// WINDOW: opening a window beyond this many active ones closes the oldest. 0 is unlimited, 1 change-point detection.
    size_t maxActiveWindows = 0;
    /// WINDOW: recent rows a window keeps as examples.
    size_t maxExamples = 5;
    /// WINDOW: the field written as a window's start; empty writes ''.
    std::string timestampField;
    /// WINDOW: windows scored per request; 0 or 1 is one request per active window, as llm_operator's batch_size 0.
    size_t windowsPerCall = 0;

    bool operator==(const SemanticKeyedConfig&) const = default;
};

/// `LLM.ANSWER_MODE`: how the model is asked and how its answer is read.
enum class AnswerMode : uint8_t
{
    /// The prompt layout of llm_operator (operator prompt, instruction, data as JSON) and a JSON answer per row. The default.
    JSON,
    /// One row per request, the prompt `<PROMPT> Answer only with <v1> or <v2>.\n\n<INPUT_LABEL>: <text>`, thinking off and
    /// one answer token (`max_tokens` 1 unless LLM.MAX_TOKENS says otherwise). A FILTER step's values are `true` and
    /// `false`, a MAP step's its OUTPUT_VALUES. This is the prompt of the distilled students of llm_operator's
    /// `finetune_label_token.py`, which are trained to answer it with one token (`SemanticLabelAnswer.hpp`).
    LABEL,
};

/// `LLM.PROXY_*`: a SEM_FILTER cascade (docs/technical/semantic_proxy.md). A smaller proxy model, the student, answers
/// every row first, in LABEL mode with logprobs. A row whose confidence max(p, 1 - p) is below the threshold, p the
/// student's probability of `true`, also goes to the model itself, the teacher, whose verdict counts; so does a random
/// audit sample of the other rows. Every other row keeps the student's verdict without a teacher request.
struct SemanticProxyConfig
{
    /// `LLM.PROXY_MODEL`: the student's model name at its endpoint.
    std::string modelName;
    /// `LLM.PROXY_ENDPOINT`: the student's endpoint; empty means the model's own ENDPOINT. For the mock, its behaviour.
    std::string endpoint;
    /// `LLM.PROXY_THRESHOLD`, between 0.5 and 1: the student answers alone if max(p, 1 - p) >= threshold.
    double threshold = 0.9;
    /// `LLM.PROXY_AUDIT_RATE`, between 0 and 1: the share of the student's sure rows the teacher also answers. A row is
    /// picked by a hash of its input text, so the same rows are audited in every run.
    double auditRate = 0.0;

    bool operator==(const SemanticProxyConfig&) const = default;
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
    /// Upper bound on the tokens one answer may generate, sent as the request's `max_tokens`. 0 sends none.
    size_t maxTokens = 0;
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
    /// A row that a batched answer leaves out gets the step's default value, as in llm_operator. True asks the model again
    /// for that row on its own instead, one more request per missing row.
    bool retryMissingRows = false;
    /// Requests in flight that every SEM_CALL to the same endpoint and model shares, across all stages and queries of the
    /// worker, as llm_operator's one semaphore per model. CREATE SEM_MODEL defaults it to MAX_CONCURRENCY. 0 shares
    /// nothing; MAX_CONCURRENCY still caps each stage.
    size_t sharedConcurrency = 0;
    /// Set for SEM_SCAN and SEM_WINDOW models.
    std::optional<SemanticKeyedConfig> keyed = std::nullopt;
    /// `LLM.IMAGE_FIELDS`: the INPUT fields, canonical names, whose values are paths of image files. SEM_CALL reads each
    /// file and attaches it to the request (`SemanticImage.hpp`); the prompt text shows "[image <n>]" in its place.
    /// SEM_MAP and SEM_FILTER only, one row per request, never fused.
    std::vector<std::string> imageFields;
    /// `LLM.IMAGE_ROOT`: the directory relative image paths are resolved against; empty is the worker's working directory.
    std::string imageRoot;
    /// `LLM.IMAGE_MAX_BYTES`: a larger file is not sent; its row gets the default value.
    size_t imageMaxBytes = DefaultImageMaxBytes;
    /// `LLM.ANSWER_MODE`. LABEL: SEM_MAP and SEM_FILTER only, one step, one row per request, never fused.
    AnswerMode answerMode = AnswerMode::JSON;
    /// `LLM.INPUT_LABEL`: LABEL mode's name for the input in the prompt, as "Review" in "Review: <text>". Empty: the INPUT
    /// field names, joined by ", ".
    std::string inputLabel;
    /// `LLM.CONFIDENCE_FIELD`, canonical: LABEL mode only. SEM_CALL asks for the logprobs of the answer token and writes,
    /// under this name, the probability of `true` (FILTER) or of the value answered (MAP), renormalized over the step's
    /// values (`labelProbability`); empty when the server sent no logprobs for any value. The field is the last of the
    /// model's OUTPUT fields, after the MAP step's. Empty: no such field and no logprobs requested.
    std::string confidenceField;
    /// `LLM.PROXY_*`: SEM_FILTER cascade with a student model. One FILTER step, BATCH_SIZE 1, never fused.
    std::optional<SemanticProxyConfig> proxy = std::nullopt;

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
