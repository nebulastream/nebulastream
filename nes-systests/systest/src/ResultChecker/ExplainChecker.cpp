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

#include <ResultChecker/ExplainChecker.hpp>

#include <algorithm>
#include <cstddef>
#include <expected>
#include <optional>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Model/Verdict.hpp>

namespace NES
{

namespace
{

constexpr std::string_view RegexOpen = "<REGEX>";
constexpr std::string_view RegexClose = "</REGEX>";
constexpr std::string_view NegativeRegexOpen = "<!REGEX>";
constexpr std::string_view NegativeRegexClose = "</!REGEX>";

struct ExplainRegexTags
{
    std::string_view opening;
    std::string_view closing;
    bool shouldMatch;
};

struct ExplainRegexAssertion
{
    std::string pattern;
    bool shouldMatch = false;
    size_t line = 0;
};

bool containsExplainRegexTag(const std::string_view line)
{
    return line.contains(RegexOpen) || line.contains(RegexClose) || line.contains(NegativeRegexOpen) || line.contains(NegativeRegexClose);
}

std::optional<ExplainRegexTags> explainRegexTags(const std::string_view line)
{
    if (line.starts_with(RegexOpen))
    {
        return ExplainRegexTags{.opening = RegexOpen, .closing = RegexClose, .shouldMatch = true};
    }
    if (line.starts_with(NegativeRegexOpen))
    {
        return ExplainRegexTags{.opening = NegativeRegexOpen, .closing = NegativeRegexClose, .shouldMatch = false};
    }
    return std::nullopt;
}

std::string explainRegexSyntaxError(const size_t line, const std::string_view message)
{
    return fmt::format("\n\nInvalid Explain Regex Assertion (line {}): {}", line + 1, message);
}

std::expected<ExplainRegexAssertion, std::string>
parseSingleLineExplainRegex(const std::string_view line, const ExplainRegexTags& tags, const size_t assertionLine)
{
    if (!line.ends_with(tags.closing))
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, fmt::format("inline assertion must end with {}", tags.closing)));
    }

    const auto pattern = line.substr(tags.opening.size(), line.size() - tags.opening.size() - tags.closing.size());
    if (containsExplainRegexTag(pattern))
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "nested or mismatched regex tag"));
    }
    if (pattern.empty())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "regex must not be empty"));
    }
    return ExplainRegexAssertion{.pattern = std::string{pattern}, .shouldMatch = tags.shouldMatch, .line = assertionLine};
}

std::expected<ExplainRegexAssertion, std::string>
parseMultilineExplainRegex(const std::vector<std::string>& expected, size_t& expectedLineIndex, const ExplainRegexTags& tags)
{
    const auto assertionLine = expectedLineIndex++;
    std::string pattern;
    while (expectedLineIndex < expected.size() && expected.at(expectedLineIndex) != tags.closing)
    {
        if (containsExplainRegexTag(expected.at(expectedLineIndex)))
        {
            return std::unexpected(explainRegexSyntaxError(expectedLineIndex, "nested or mismatched regex tag"));
        }
        if (!pattern.empty())
        {
            pattern += '\n';
        }
        pattern += expected.at(expectedLineIndex++);
    }

    if (expectedLineIndex == expected.size())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, fmt::format("missing closing tag {}", tags.closing)));
    }
    ++expectedLineIndex;
    if (pattern.empty())
    {
        return std::unexpected(explainRegexSyntaxError(assertionLine, "regex must not be empty"));
    }
    return ExplainRegexAssertion{.pattern = std::move(pattern), .shouldMatch = tags.shouldMatch, .line = assertionLine};
}

std::expected<std::vector<ExplainRegexAssertion>, std::string> parseExplainRegexAssertions(const std::vector<std::string>& expected)
{
    std::vector<ExplainRegexAssertion> assertions;
    size_t expectedLineIndex = 0;
    while (expectedLineIndex < expected.size())
    {
        const auto line = std::string_view{expected.at(expectedLineIndex)};
        const auto tags = explainRegexTags(line);
        if (!tags)
        {
            return std::unexpected(explainRegexSyntaxError(expectedLineIndex, "tagged and untagged expected output must not be mixed"));
        }

        std::expected<ExplainRegexAssertion, std::string> assertion = line == tags->opening
            ? parseMultilineExplainRegex(expected, expectedLineIndex, *tags)
            : parseSingleLineExplainRegex(line, *tags, expectedLineIndex++);
        if (!assertion)
        {
            return std::unexpected(std::move(assertion).error());
        }
        assertions.emplace_back(std::move(assertion).value());
    }
    return assertions;
}

Verdict checkExplainRegexAssertions(const std::vector<ExplainRegexAssertion>& assertions, const std::string& actualOutput)
{
    for (const auto& [pattern, shouldMatch, line] : assertions)
    {
        try
        {
            if (std::regex_search(actualOutput, std::regex(pattern)) != shouldMatch)
            {
                return std::unexpected(Mismatch{fmt::format(
                    "\n\n"
                    "Explain Output Regex Assertion Failed (line {}, expected pattern \"{}\" {} match)\n"
                    "----------------------\n"
                    "Actual:\n{}",
                    line + 1,
                    pattern,
                    shouldMatch ? "to" : "not to",
                    actualOutput)});
            }
        }
        catch (const std::regex_error& exception)
        {
            return std::unexpected(Mismatch{
                fmt::format("\n\nInvalid Explain Output Regex (line {}, pattern \"{}\"): {}", line + 1, pattern, exception.what())});
        }
    }
    return {};
}

/// Right-trims every line and drops the ones left empty.
/// Indentation matters in a plan, but an expected block holds no empty line, because an empty line ends the block.
template <typename Lines>
std::vector<std::string> normalize(Lines&& lines)
{
    std::vector<std::string> normalized;
    for (const std::string_view line : std::forward<Lines>(lines))
    {
        const auto end = line.find_last_not_of(" \t\r");
        if (end != std::string_view::npos)
        {
            normalized.emplace_back(line.substr(0, end + 1));
        }
    }
    return normalized;
}

/// Splits a printed plan into trimmed lines.
std::vector<std::string> normalizedLinesOf(const std::string& output)
{
    return normalize(
        output | std::views::split('\n')
        | std::views::transform([](auto&& split) { return std::string_view(split.begin(), split.end()); }));
}

/// Reports the first differing line, and both plans in full so the reader sees the difference in context.
Verdict reportFirstDifference(const std::vector<std::string>& expected, const std::vector<std::string>& actual)
{
    static constexpr std::string_view EndOfOutput = "<end of output>";
    const size_t firstDifferingLine = std::ranges::mismatch(expected, actual).in1 - expected.begin();
    return std::unexpected(Mismatch{fmt::format(
        "\n\n"
        "Explain Output Mismatch (first difference at line {}, expected \"{}\" but got \"{}\")\n"
        "----------------------\n"
        "Expected:\n{}\n\n"
        "Actual:\n{}",
        firstDifferingLine + 1,
        firstDifferingLine < expected.size() ? std::string_view{expected.at(firstDifferingLine)} : EndOfOutput,
        firstDifferingLine < actual.size() ? std::string_view{actual.at(firstDifferingLine)} : EndOfOutput,
        fmt::join(expected, "\n"),
        fmt::join(actual, "\n"))});
}

}

Verdict ExplainCheck::check() const
{
    const auto expectedLines = normalize(expected);
    const auto actualLines = normalizedLinesOf(actualOutput);

    if (std::ranges::any_of(expectedLines, [](const auto& line) { return containsExplainRegexTag(line); }))
    {
        auto assertions = parseExplainRegexAssertions(expectedLines);
        if (!assertions)
        {
            return std::unexpected(Mismatch{std::move(assertions).error()});
        }
        return checkExplainRegexAssertions(assertions.value(), fmt::format("{}", fmt::join(actualLines, "\n")));
    }

    if (expectedLines == actualLines)
    {
        return {};
    }
    return reportFirstDifference(expectedLines, actualLines);
}

}
