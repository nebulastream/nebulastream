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

}
