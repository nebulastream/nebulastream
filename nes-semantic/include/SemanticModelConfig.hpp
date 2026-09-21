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
#include <vector>

namespace NES
{

/// One operation applied to a row. SEM_MAP always has exactly one step; the list shape exists so
/// operator fusion (SEM_FILTER, cascades) has somewhere to put a second step without reopening the
/// catalog struct, the reflected wire format, the DDL and the prompt builder together later.
struct SemanticStep
{
    enum class Kind : uint8_t
    {
        MAP /// FILTER arrives with SEM_FILTER
    };

    Kind kind = Kind::MAP;
    /// User-authored operator instruction, e.g. "Classify the sentiment as POSITIVE or NEGATIVE".
    std::string prompt;
    /// Restricted value set for answer normalisation (`_normalize_answer`, plan §2.3).
    /// Empty means free text: no normalisation is applied.
    std::vector<std::string> outputValues;
    /// Value substituted when the LLM's answer for this step could not be parsed or normalised.
    /// Always "" today.
    std::string defaultValue;

    bool operator==(const SemanticStep&) const = default;
};

/// User-supplied `CREATE SEMANTIC MODEL ... SET (...)` options, carried verbatim from the DDL
/// options clause through to the LLM client. Endpoint reachability is intentionally not
/// validated here or at CREATE time — see plan §M3, "do not contact the endpoint at CREATE time".
struct SemanticModelConfig
{
    /// OpenAI-compatible base URL, e.g. "http://host.docker.internal:11434/v1".
    std::string baseUrl;
    /// Model identifier passed through to the endpoint, e.g. "gemma3:27b".
    std::string model;
    /// Name of an environment variable holding the API key. Never a literal key in SQL.
    std::optional<std::string> apiKeyEnv;
    /// Exactly one entry per declared OUTPUT field; step i pairs with output i (SEM_MAP: one of each).
    std::vector<SemanticStep> steps;
    /// Rows batched per HTTP request. >1 requires the asynchronous execution path (stage 2);
    /// registerModel rejects it until that lands.
    size_t batchSize = 1;
    /// In-flight HTTP requests per worker. >1 requires the asynchronous execution path (stage 2);
    /// registerModel rejects it until that lands.
    size_t maxConcurrency = 1;
    /// Retries on transport failure and 429/5xx only; 4xx is not retried.
    size_t maxRetries = 2;
    std::chrono::seconds requestTimeout{600};
    std::chrono::milliseconds connectTimeout{10000};

    bool operator==(const SemanticModelConfig&) const = default;
};

}
