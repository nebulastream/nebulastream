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

#include <SemanticJoinCodec.hpp>

#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ResponseParsing.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticPromptLayout.hpp>

namespace NES
{

SemanticJoinCodec::SemanticJoinCodec(SemanticModelConfig config) : config(std::move(config))
{
    PRECONDITION(
        this->config.steps.size() == 1 && this->config.steps.front().kind == SemanticStep::Kind::FILTER,
        "SemanticJoinCodec needs exactly one FILTER step, got {} steps",
        this->config.steps.size());
}

std::string SemanticJoinCodec::buildPrompt(const std::span<const JoinPairPayload> pairs) const
{
    /// `self.operator_prompt` of `SEMJoinOperator`; the instruction and the sentence after it are joined by one space.
    auto prompt = fmt::format(
        "{}\nEvaluate the following pairs (left,right) with this user instruction: {} "
        "Return JSON only, keyed by each pair's row id as instructed above, with "
        "{{\"answer\": true/false, \"confidence\": float}} as the value.\n",
        detail::BaseSysprompt,
        config.steps.front().prompt);
    /// `_dataset_context_block`: the reference has a context per side, each falling back to the shared one, which is
    /// all a semantic model declares.
    if (!config.datasetPrompt.empty())
    {
        prompt += fmt::format("Left dataset context: {0}\nRight dataset context: {0}\n", config.datasetPrompt);
    }
    prompt += "Data: ";
    detail::appendPythonJsonObject(
        prompt,
        pairs,
        [](std::string& out, const JoinPairPayload& pair)
        {
            detail::appendPythonJsonString(out, pair.pairId);
            out += R"(: {"left": )";
            detail::appendPythonJsonString(out, pair.left);
            out += R"(, "right": )";
            detail::appendPythonJsonString(out, pair.right);
            out.push_back('}');
        });
    return prompt;
}

std::vector<bool> SemanticJoinCodec::parse(const std::string_view response, const std::span<const JoinPairPayload> pairs) const
{
    const auto parsed = parseLlmJson(response);
    std::vector<bool> matches;
    matches.reserve(pairs.size());
    for (const auto& pair : pairs)
    {
        /// `isinstance(llm_result, dict) and llm_result.get("answer") is True`: stricter than SEM_FILTER's truthiness.
        bool matched = false;
        if (parsed.is_object() && parsed.contains(pair.pairId))
        {
            const auto& entry = parsed.at(pair.pairId);
            if (entry.is_object())
            {
                const auto answer = entry.find("answer");
                matched = answer != entry.end() && answer->is_boolean() && answer->get<bool>();
            }
        }
        if (!matched)
        {
            NES_TRACE("Semantic join drops pair {}", pair.pairId);
        }
        matches.push_back(matched);
    }
    return matches;
}

}
