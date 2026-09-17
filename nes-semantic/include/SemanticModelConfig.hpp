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

#include <optional>
#include <string>
#include <vector>

namespace NES
{

/// User-supplied `CREATE SEMANTIC MODEL ... SET (...)` options, carried verbatim from the DDL
/// options clause through to the LLM client. Endpoint reachability is intentionally not
/// validated here or at CREATE time — see plan §M3, "do not contact the endpoint at CREATE time".
struct SemanticModelConfig
{
    /// OpenAI-compatible base URL, e.g. "http://host.docker.internal:11434/v1".
    std::string baseUrl;
    /// Model identifier passed through to the endpoint, e.g. "gemma3:27b".
    std::string model;
    /// User-authored operator instruction, e.g. "Classify the sentiment as POSITIVE or NEGATIVE".
    std::string prompt;
    /// Name of an environment variable holding the API key. Never a literal key in SQL.
    std::optional<std::string> apiKeyEnv;
    /// Restricted value set for answer normalisation (`_normalize_answer`, plan §2.3).
    /// Absent means free text: no normalisation is applied.
    std::optional<std::vector<std::string>> outputValues;

    bool operator==(const SemanticModelConfig&) const = default;
};

}
