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

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

/// `LLM.ANSWER_MODE 'LABEL'`: the prompt and answer of the distilled students in llm_operator
/// (`experiments/scripts/finetune_label_token.py`). The student is trained on exactly
///
///     <instruction> Answer only with true or false.\n\nReview: <text>
///
/// as one user message, rendered with thinking off, and answers with one token. NES sends the same prompt, with the
/// model's PROMPT as the instruction, LLM.INPUT_LABEL as "Review", and the step's admissible values in place of
/// "true or false"; the answer is read as one of those values, without any JSON envelope.

/// The values a LABEL step answers with: `true` and `false` for a FILTER step, the OUTPUT_VALUES of a MAP step.
[[nodiscard]] std::vector<std::string> labelValues(const SemanticStep& step);

/// "Answer only with true or false." for two values, "Answer only with a, b or c." for more.
[[nodiscard]] std::string labelAnswerInstruction(const std::vector<std::string>& values);

/// `<instruction> <answer instruction>\n\n<inputLabel>: <text>`.
[[nodiscard]] std::string
buildLabelPrompt(std::string_view instruction, const std::vector<std::string>& values, std::string_view inputLabel, std::string_view text);

/// The value the answer names: the text without surrounding whitespace, quotes and a trailing '.', compared with each
/// value ignoring case. Nothing for any other answer, which the caller turns into the default (a filter's `false`).
[[nodiscard]] std::optional<std::string> parseLabelAnswer(std::string_view content, const std::vector<std::string>& values);

/// The request fields of LABEL mode, added to a chat request: `max_tokens` (1 unless `maxTokens` is set) and
/// `chat_template_kwargs.enable_thinking` false, the student's training setting. Servers that do not know
/// `chat_template_kwargs` ignore it (vLLM applies it, Ollama ignores it).
void applyLabelRequestFields(nlohmann::json& request, size_t maxTokens);

/// How many alternatives per position LLM.CONFIDENCE_FIELD asks for: vLLM's default limit (`--max-logprobs 20`).
constexpr size_t LabelTopLogprobs = 20;

/// The answer token's alternatives, (token, logprob), as `top_logprobs` lists them.
using TokenLogprobs = std::vector<std::pair<std::string, double>>;

/// LLM.CONFIDENCE_FIELD: additionally `logprobs` true and `top_logprobs` LabelTopLogprobs.
void applyLogprobRequestFields(nlohmann::json& request);

/// `choices[0].logprobs.content[0].top_logprobs` of a chat-completion envelope (the OpenAI shape vLLM and Ollama send):
/// the alternatives at the first generated token, the answer position. Empty when the envelope has none.
[[nodiscard]] TokenLogprobs firstTokenLogprobs(const nlohmann::json& envelope);

/// The probability of `target` renormalized over `values`: each value's mass is the summed probability of the listed
/// tokens that spell it, ignoring case and surrounding whitespace ("true", "True", " true"), then normalized over the
/// values. For a student trained on the lowercase tokens this is the training script's softmax over the [false, true]
/// logits up to the variants' tiny mass; an untrained model that answers "True" gets a usable number too. A value with
/// no listed token counts as 0; nothing if no value is listed at all, or `target` is not a value.
[[nodiscard]] std::optional<double>
labelProbability(const TokenLogprobs& alternatives, const std::vector<std::string>& values, std::string_view target);

/// The probability as the confidence column holds it: six decimals, e.g. "0.973105".
[[nodiscard]] std::string formatProbability(double probability);

}
