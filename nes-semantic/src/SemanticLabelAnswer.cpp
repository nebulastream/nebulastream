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

#include <SemanticLabelAnswer.hpp>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

bool equalsIgnoringCase(const std::string_view lhs, const std::string_view rhs)
{
    return lhs.size() == rhs.size()
        && std::ranges::equal(
               lhs,
               rhs,
               [](const unsigned char left, const unsigned char right) { return std::tolower(left) == std::tolower(right); });
}

/// Whitespace, quotes and backticks around the answer, and one trailing '.', as in "True." or "`false`".
std::string_view trimAnswer(std::string_view text)
{
    const auto strip = [](const unsigned char character)
    { return std::isspace(character) != 0 || character == '"' || character == '\'' || character == '`'; };
    while (!text.empty() && strip(static_cast<unsigned char>(text.front())))
    {
        text.remove_prefix(1);
    }
    while (!text.empty() && strip(static_cast<unsigned char>(text.back())))
    {
        text.remove_suffix(1);
    }
    if (text.ends_with('.'))
    {
        text.remove_suffix(1);
        while (!text.empty() && strip(static_cast<unsigned char>(text.back())))
        {
            text.remove_suffix(1);
        }
    }
    return text;
}

}

std::vector<std::string> labelValues(const SemanticStep& step)
{
    if (step.kind == SemanticStep::Kind::FILTER)
    {
        return {"true", "false"};
    }
    return step.outputValues;
}

std::string labelAnswerInstruction(const std::vector<std::string>& values)
{
    if (values.size() < 2)
    {
        return fmt::format("Answer only with {}.", values.empty() ? std::string{} : values.front());
    }
    std::string listed;
    for (size_t index = 0; index + 1 < values.size(); ++index)
    {
        if (index > 0)
        {
            listed += ", ";
        }
        listed += values[index];
    }
    return fmt::format("Answer only with {} or {}.", listed, values.back());
}

std::string buildLabelPrompt(
    const std::string_view instruction, const std::vector<std::string>& values, const std::string_view inputLabel, const std::string_view text)
{
    return fmt::format("{} {}\n\n{}: {}", instruction, labelAnswerInstruction(values), inputLabel, text);
}

std::optional<std::string> parseLabelAnswer(const std::string_view content, const std::vector<std::string>& values)
{
    const auto answer = trimAnswer(content);
    for (const auto& value : values)
    {
        if (equalsIgnoringCase(answer, value))
        {
            return value;
        }
    }
    return std::nullopt;
}

void applyLabelRequestFields(nlohmann::json& request, const size_t maxTokens)
{
    request["max_tokens"] = maxTokens > 0 ? maxTokens : 1;
    request["chat_template_kwargs"] = {{"enable_thinking", false}};
}

void applyLogprobRequestFields(nlohmann::json& request)
{
    request["logprobs"] = true;
    request["top_logprobs"] = LabelTopLogprobs;
}

TokenLogprobs firstTokenLogprobs(const nlohmann::json& envelope)
{
    TokenLogprobs alternatives;
    if (!envelope.is_object() || !envelope.contains("choices") || !envelope["choices"].is_array() || envelope["choices"].empty())
    {
        return alternatives;
    }
    const auto& choice = envelope["choices"][0];
    if (!choice.is_object() || !choice.contains("logprobs") || !choice["logprobs"].is_object())
    {
        return alternatives;
    }
    const auto& content = choice["logprobs"].value("content", nlohmann::json{});
    if (!content.is_array() || content.empty() || !content[0].is_object())
    {
        return alternatives;
    }
    const auto& first = content[0];
    const auto& top = first.value("top_logprobs", nlohmann::json{});
    if (top.is_array() && !top.empty())
    {
        for (const auto& entry : top)
        {
            if (entry.is_object() && entry.contains("token") && entry["token"].is_string() && entry.contains("logprob")
                && entry["logprob"].is_number())
            {
                alternatives.emplace_back(entry["token"].get<std::string>(), entry["logprob"].get<double>());
            }
        }
        return alternatives;
    }
    /// No alternatives requested or sent: the generated token alone.
    if (first.contains("token") && first["token"].is_string() && first.contains("logprob") && first["logprob"].is_number())
    {
        alternatives.emplace_back(first["token"].get<std::string>(), first["logprob"].get<double>());
    }
    return alternatives;
}

std::optional<double>
labelProbability(const TokenLogprobs& alternatives, const std::vector<std::string>& values, const std::string_view target)
{
    if (std::ranges::find(values, target) == values.end())
    {
        return std::nullopt;
    }
    /// Each spelling of a value, relative to the most likely listed one so that very negative logprobs do not underflow.
    double highest = -INFINITY;
    for (const auto& [token, logprob] : alternatives)
    {
        if (std::isfinite(logprob))
        {
            highest = std::max(highest, logprob);
        }
    }
    std::vector<double> mass(values.size(), 0.0);
    bool listed = false;
    for (const auto& [token, logprob] : alternatives)
    {
        if (!std::isfinite(logprob))
        {
            continue;
        }
        std::string_view spelled{token};
        while (!spelled.empty() && std::isspace(static_cast<unsigned char>(spelled.front())) != 0)
        {
            spelled.remove_prefix(1);
        }
        while (!spelled.empty() && std::isspace(static_cast<unsigned char>(spelled.back())) != 0)
        {
            spelled.remove_suffix(1);
        }
        for (size_t index = 0; index < values.size(); ++index)
        {
            if (equalsIgnoringCase(spelled, values[index]))
            {
                mass[index] += std::exp(logprob - highest);
                listed = true;
            }
        }
    }
    if (!listed)
    {
        return std::nullopt;
    }
    double total = 0;
    double targetMass = 0;
    for (size_t index = 0; index < values.size(); ++index)
    {
        total += mass[index];
        if (values[index] == target)
        {
            targetMass = mass[index];
        }
    }
    return targetMass / total;
}

std::string formatProbability(const double probability)
{
    return fmt::format("{:.6f}", probability);
}

}
