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

#include <ResponseParsing.hpp>

#include <algorithm>
#include <cctype>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace NES
{

namespace
{

std::optional<nlohmann::json> tryParseJson(const std::string_view text)
{
    auto parsed = nlohmann::json::parse(text, nullptr, false);
    if (parsed.is_discarded())
    {
        return std::nullopt;
    }
    return parsed;
}

constexpr std::string_view Whitespace = " \t\r\n\f\v";

std::string_view trim(std::string_view text)
{
    const auto begin = text.find_first_not_of(Whitespace);
    if (begin == std::string_view::npos)
    {
        return {};
    }
    const auto end = text.find_last_not_of(Whitespace);
    return text.substr(begin, end - begin + 1);
}

bool equalsIgnoreCase(const std::string_view lhs, const std::string_view rhs)
{
    return std::ranges::equal(
        lhs, rhs, [](const unsigned char left, const unsigned char right) { return std::tolower(left) == std::tolower(right); });
}

std::string toUpper(std::string_view text)
{
    std::string result(text);
    std::ranges::transform(
        result, result.begin(), [](const unsigned char character) { return static_cast<char>(std::toupper(character)); });
    return result;
}

/// Contents of the first markdown fence, as `` ```(?:json)?\s*(.*?)\s*``` `` with DOTALL would capture.
/// Written out rather than as a std::regex: libstdc++'s regex engine recurses per character and
/// overflows the stack on responses of a few ten kilobytes.
std::optional<std::string_view> firstCodeFence(const std::string_view text)
{
    constexpr std::string_view Fence = "```";
    const auto open = text.find(Fence);
    if (open == std::string_view::npos)
    {
        return std::nullopt;
    }
    auto inner = text.substr(open + Fence.size());
    const auto close = inner.find(Fence);
    if (close == std::string_view::npos)
    {
        return std::nullopt;
    }
    inner = inner.substr(0, close);
    if (inner.starts_with("json"))
    {
        inner.remove_prefix(4);
    }
    return trim(inner);
}

/// The greedy DOTALL `\{.*\}`: first opening to last closing brace.
std::optional<std::string_view> outermostObject(const std::string_view text)
{
    const auto open = text.find('{');
    const auto close = text.rfind('}');
    if (open == std::string_view::npos || close == std::string_view::npos || close < open)
    {
        return std::nullopt;
    }
    return text.substr(open, close - open + 1);
}

}

nlohmann::json parseLlmJson(const std::string_view response)
{
    if (auto parsed = tryParseJson(response))
    {
        return std::move(parsed).value();
    }
    if (const auto fence = firstCodeFence(response))
    {
        if (auto parsed = tryParseJson(*fence))
        {
            return std::move(parsed).value();
        }
    }
    if (const auto object = outermostObject(response))
    {
        if (auto parsed = tryParseJson(*object))
        {
            return std::move(parsed).value();
        }
    }

    /// An echoed `"_llm_call_id": "row1", {...}` wrapper and `"row1", {...}` with a comma instead of a
    /// colon; stripping the first reduces it to the second. Both patterns only span one quoted string,
    /// so std::regex stays shallow here.
    static const std::regex strayIdKey(R"("_llm_call_id"\s*:\s*(?="))");
    static const std::regex missingColon(R"re("([^"]+)"\s*,\s*(?=\{))re");
    const std::string original(response);
    std::string repaired = std::regex_replace(original, strayIdKey, "");
    repaired = std::regex_replace(repaired, missingColon, R"("$1": )");
    if (repaired != original)
    {
        if (auto parsed = tryParseJson(repaired))
        {
            return std::move(parsed).value();
        }
    }
    return nlohmann::json::object();
}

std::string
normalizeAnswer(const std::string_view answer, const std::vector<std::string>& outputValues, const std::string_view defaultValue)
{
    if (outputValues.empty())
    {
        return std::string(answer);
    }

    /// Declaration order is kept so containment ties break like the reference's insertion-ordered dict.
    std::vector<std::pair<std::string, std::string>> lookup;
    lookup.reserve(outputValues.size());
    for (const auto& value : outputValues)
    {
        lookup.emplace_back(toUpper(value), value);
    }
    const auto findExact = [&lookup](const std::string& key) -> std::optional<std::string>
    {
        const auto it = std::ranges::find(lookup, key, &std::pair<std::string, std::string>::first);
        return it != lookup.end() ? std::make_optional(it->second) : std::nullopt;
    };

    const auto upper = toUpper(trim(answer));
    if (auto exact = findExact(upper))
    {
        return std::move(exact).value();
    }

    static const std::regex parenthetical(R"(\s*\(.*?\))");
    if (auto exact = findExact(std::string(trim(std::regex_replace(upper, parenthetical, "")))))
    {
        return std::move(exact).value();
    }

    for (const auto& [key, value] : lookup)
    {
        if (upper.find(key) != std::string::npos)
        {
            return value;
        }
    }
    return std::string(defaultValue);
}

const nlohmann::json* findMember(const nlohmann::json& object, const std::string_view key)
{
    if (!object.is_object())
    {
        return nullptr;
    }
    if (const auto exact = object.find(std::string(key)); exact != object.end())
    {
        return &*exact;
    }
    for (const auto& [memberKey, value] : object.items())
    {
        if (equalsIgnoreCase(memberKey, key))
        {
            return &value;
        }
    }
    return nullptr;
}

bool isAffirmative(const nlohmann::json& verdict)
{
    if (verdict.is_boolean())
    {
        return verdict.get<bool>();
    }
    if (verdict.is_number())
    {
        return verdict.get<double>() != 0.0;
    }
    if (verdict.is_string())
    {
        const auto text = trim(verdict.get_ref<const std::string&>());
        return equalsIgnoreCase(text, "true") || equalsIgnoreCase(text, "yes");
    }
    return false;
}

}
