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
#include <string_view>
#include <unordered_map>

namespace NES
{

/// One output field's answer, after JSON extraction and answer normalisation (plan §2.2, §2.3).
struct SemanticFieldResult
{
    std::string answer;
    double confidence = 0.0;

    bool operator==(const SemanticFieldResult&) const = default;
};

/// Keyed by declared OUTPUT field base name (e.g. "sentiment", not "sentiment_confidence").
using SemanticMapResult = std::unordered_map<std::string, SemanticFieldResult>;

/// Sends one row's INPUT content to a semantic model endpoint and returns one `SemanticFieldResult`
/// per declared OUTPUT field. A virtual interface from day one — `SemMapPhysicalOperator` (plan §M1)
/// is built against this, not against `CurlLlmClient` directly, so a stub implementation can stand
/// in for both `SemMapPhysicalOperatorTest` (plan §M1) and the hermetic systest (plan §M4). That
/// substitutability is the entire hermeticity story for this operator.
class LlmClient
{
public:
    virtual ~LlmClient() = default;

    /// `inputText` is the space-joined, already-stringified INPUT field values for one row
    /// (plan §2.1's data block, single-row case). Blocks the calling thread for the full round
    /// trip — Phase 1 has no async path (plan §1, §D6).
    virtual SemanticMapResult map(std::string_view inputText) = 0;
};

}
