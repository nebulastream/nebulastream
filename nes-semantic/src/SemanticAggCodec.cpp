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

#include <SemanticAggCodec.hpp>

#include <regex>
#include <string>
#include <string_view>
#include <utility>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <ErrorHandling.hpp>
#include <SemanticModelCatalog.hpp>
#include <SemanticPromptLayout.hpp>

namespace NES
{

namespace
{
/// `_SYSPROMPT` of `sem_agg.py`, with `_FORMAT_RULE` appended.
constexpr std::string_view AggregationSysprompt
    = "You are a helpful AI assistant for semantic data aggregation.\n"
      "You receive a window of records and must produce a single aggregate result for the entire window.\n"
      "Respond with the aggregate result according to the aggregation instruction.\n"
      "Do not add explanations unless the instruction specifically asks for them.\n"
      "Use exactly the output format the aggregation instruction asks for (for example a JSON object); "
      "if it names none, answer in plain text. Never wrap the answer in markdown code fences (```).";

std::string_view trim(std::string_view text)
{
    const auto first = text.find_first_not_of(" \t\n\r\f\v");
    if (first == std::string_view::npos)
    {
        return {};
    }
    return text.substr(first, text.find_last_not_of(" \t\n\r\f\v") - first + 1);
}
}

SemanticAggCodec::SemanticAggCodec(SemanticModelConfig config, std::string inputField)
    : config(std::move(config)), inputField(std::move(inputField))
{
    PRECONDITION(
        this->config.steps.size() == 1 && this->config.steps.front().kind == SemanticStep::Kind::MAP,
        "SemanticAggCodec needs exactly one MAP step, got {} steps",
        this->config.steps.size());
}

std::string SemanticAggCodec::buildPrompt(const std::string_view values) const
{
    const auto parsed = nlohmann::json::parse(values, nullptr, false);
    if (!parsed.is_array())
    {
        throw CannotDeserialize("SEM_AGG expects a JSON array of strings, got {}", values.substr(0, 200));
    }
    auto prompt = fmt::format("{}\nAggregation instruction: {}\n", AggregationSysprompt, config.steps.front().prompt);
    prompt += detail::datasetContextBlock(config);
    prompt += "Window:\n";
    /// `json.dumps([{col: value} for row in window])`
    prompt.push_back('[');
    bool first = true;
    for (const auto& value : parsed)
    {
        if (!value.is_string())
        {
            throw CannotDeserialize("SEM_AGG expects a JSON array of strings, got an element {}", value.dump());
        }
        if (!first)
        {
            prompt += ", ";
        }
        first = false;
        prompt.push_back('{');
        detail::appendPythonJsonString(prompt, inputField);
        prompt += ": ";
        detail::appendPythonJsonString(prompt, value.get_ref<const std::string&>());
        prompt.push_back('}');
    }
    prompt.push_back(']');
    return prompt;
}

std::string SemanticAggCodec::parse(const std::string_view response) const
{
    /// `_FENCE_RE`: `^\s*```[\w-]*[ \t]*\n(.*?)\n?```\s*$` with DOTALL, the group stripped.
    static const std::regex fence{R"(^\s*```[\w-]*[ \t]*\n([\s\S]*?)\n?```\s*$)"};
    std::string answer{response};
    std::smatch match;
    if (std::regex_match(answer, match, fence))
    {
        answer = std::string{trim(match[1].str())};
    }
    return answer.empty() ? config.steps.front().defaultValue : answer;
}

}
