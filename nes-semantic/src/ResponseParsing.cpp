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

namespace NES
{

namespace
{

std::optional<nlohmann::json> tryParseJson(const std::string& text)
{
    auto parsed = nlohmann::json::parse(text, nullptr, false);
    if (parsed.is_discarded())
    {
        return std::nullopt;
    }
    return parsed;
}

std::string toUpper(std::string_view text)
{
    std::string result(text);
    std::ranges::transform(result, result.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return result;
}

std::string trim(std::string_view text)
{
    const auto begin = text.find_first_not_of(" \t\r\n");
    if (begin == std::string_view::npos)
    {
        return "";
    }
    const auto end = text.find_last_not_of(" \t\r\n");
    return std::string(text.substr(begin, end - begin + 1));
}

/// Levenshtein edit distance between two strings.
size_t levenshteinDistance(std::string_view a, std::string_view b)
{
    std::vector<size_t> previous(b.size() + 1);
    std::vector<size_t> current(b.size() + 1);
    for (size_t j = 0; j <= b.size(); ++j)
    {
        previous[j] = j;
    }
    for (size_t i = 1; i <= a.size(); ++i)
    {
        current[0] = i;
        for (size_t j = 1; j <= b.size(); ++j)
        {
            const size_t cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
            current[j] = std::min({previous[j] + 1, current[j - 1] + 1, previous[j - 1] + cost});
        }
        std::swap(previous, current);
    }
    return previous[b.size()];
}

/// Normalised similarity in [0, 1]. Not difflib's ratio (2*M/T over matching blocks) — see
/// ResponseParsing.hpp for the divergence note.
double levenshteinRatio(std::string_view a, std::string_view b)
{
    const size_t total = a.size() + b.size();
    if (total == 0)
    {
        return 1.0;
    }
    const size_t distance = levenshteinDistance(a, b);
    return static_cast<double>(total - distance) / static_cast<double>(total);
}

}

nlohmann::json parseLlmJson(std::string_view response)
{
    const std::string text(response);

    if (auto parsed = tryParseJson(text))
    {
        return *parsed;
    }

    static const std::regex codeFenceRe(R"(```(?:json)?\s*([\s\S]*?)\s*```)");
    if (std::smatch match; std::regex_search(text, match, codeFenceRe))
    {
        if (auto parsed = tryParseJson(match[1].str()))
        {
            return *parsed;
        }
    }

    static const std::regex bareObjectRe(R"(\{[\s\S]*\})");
    if (std::smatch match; std::regex_search(text, match, bareObjectRe))
    {
        if (auto parsed = tryParseJson(match[0].str()))
        {
            return *parsed;
        }
    }

    /// Repairs two malformed id->object shapes some local models produce (plan §2.2):
    /// an echoed `"_llm_call_id"` wrapper key, and a comma used instead of a colon.
    static const std::regex strayIdKeyRe(R"("_llm_call_id"\s*:\s*(?="))");
    static const std::regex missingColonRe(R"re("([^"]+)"\s*,\s*(?=\{))re");
    std::string repaired = std::regex_replace(text, strayIdKeyRe, "");
    repaired = std::regex_replace(repaired, missingColonRe, R"("$1": )");
    if (repaired != text)
    {
        if (auto parsed = tryParseJson(repaired))
        {
            return *parsed;
        }
    }

    return nlohmann::json::object();
}

std::string
normalizeAnswer(const std::string_view answer, const std::optional<std::vector<std::string>>& outputValues, const std::string_view defaultValue)
{
    if (!outputValues.has_value() || outputValues->empty())
    {
        return std::string(answer);
    }

    /// Preserves declaration order so substring-containment ties break the same way the Python
    /// baseline's insertion-ordered dict iteration does.
    std::vector<std::pair<std::string, std::string>> lookup;
    lookup.reserve(outputValues->size());
    for (const auto& value : *outputValues)
    {
        lookup.emplace_back(toUpper(value), value);
    }
    const auto findExact = [&](const std::string& key) -> std::optional<std::string>
    {
        const auto it = std::ranges::find(lookup, key, &std::pair<std::string, std::string>::first);
        return it != lookup.end() ? std::make_optional(it->second) : std::nullopt;
    };

    const std::string upper = toUpper(trim(answer));
    if (auto exact = findExact(upper))
    {
        return *exact;
    }

    static const std::regex parentheticalRe(R"(\s*\(.*?\))");
    const std::string stripped = trim(std::regex_replace(upper, parentheticalRe, ""));
    if (auto exact = findExact(stripped))
    {
        return *exact;
    }

    for (const auto& [key, value] : lookup)
    {
        if (upper.find(key) != std::string::npos)
        {
            return value;
        }
    }

    std::optional<std::string> bestMatch;
    double bestRatio = 0.6;
    for (const auto& [key, value] : lookup)
    {
        if (const double ratio = levenshteinRatio(upper, key); ratio >= bestRatio)
        {
            bestRatio = ratio;
            bestMatch = value;
        }
    }
    if (bestMatch)
    {
        return *bestMatch;
    }

    return std::string(defaultValue);
}

}
