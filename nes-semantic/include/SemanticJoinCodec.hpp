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

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <SemanticModelCatalog.hpp>

namespace NES
{

/// One candidate pair as the join codec sees it: an id unique within one prompt, and the text of each side.
struct JoinPairPayload
{
    std::string pairId;
    std::string left;
    std::string right;
};

/// SEM_JOIN's prompt layout and response decoding, after `SEMJoinOperator._process_rows` of the Python reference:
///
///     <base sysprompt>                     the same flat {row_id: {answer, confidence}} envelope as SEM_FILTER
///     Evaluate the following pairs (left,right) with this user instruction: <prompt> Return JSON only, ...
///     [Left dataset context: ...]          the reference's per-side contexts; both are the model's dataset prompt
///     [Right dataset context: ...]
///     Data: {"<pair_id>": {"left": ..., "right": ...}, ...}
///
/// The model is a filter model with exactly one FILTER step, whose prompt is the user instruction.
class SemanticJoinCodec
{
public:
    explicit SemanticJoinCodec(SemanticModelConfig config);

    [[nodiscard]] std::string buildPrompt(std::span<const JoinPairPayload> pairs) const;

    /// One verdict per pair. As in the reference, a pair matches only if its entry is an object whose `answer` is the
    /// JSON boolean `true`; anything else, a missing pair and an unparseable response included, is no match.
    [[nodiscard]] std::vector<bool> parse(std::string_view response, std::span<const JoinPairPayload> pairs) const;

private:
    SemanticModelConfig config;
};

}
