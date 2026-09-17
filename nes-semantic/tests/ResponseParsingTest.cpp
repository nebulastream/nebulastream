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

#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{

struct ParseLlmJsonCase
{
    std::string name;
    std::string input;
    nlohmann::json expected;
};

}

class ParseLlmJsonTest : public ::testing::TestWithParam<ParseLlmJsonCase>
{
};

/// Table-driven over `_parse_llm_json`'s four fallback paths (llm_operator.py:31-74), plus total
/// failure. Each case exercises exactly one path; earlier paths are proven not to fire by giving
/// them nothing to match.
INSTANTIATE_TEST_SUITE_P(
    Fallbacks,
    ParseLlmJsonTest,
    ::testing::ValuesIn(std::vector<ParseLlmJsonCase>{
        {.name = "StraightJson",
         .input = R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.9}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.9}}})")},
        {.name = "MarkdownCodeFenceWithLanguageTag",
         .input = "```json\n{\"row1\": {\"sentiment\": {\"answer\": \"NEGATIVE\", \"confidence\": 0.5}}}\n```",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "NEGATIVE", "confidence": 0.5}}})")},
        {.name = "MarkdownCodeFenceBare",
         .input = "```\n{\"row1\": {\"sentiment\": {\"answer\": \"NEGATIVE\", \"confidence\": 0.5}}}\n```",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "NEGATIVE", "confidence": 0.5}}})")},
        {.name = "OutermostBareObjectWithSurroundingProse",
         .input = R"(Sure, here is the answer: {"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.8}}} Hope that helps!)",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.8}}})")},
        {.name = "RepairsStrayLlmCallIdWrapperKey",
         .input = R"({"_llm_call_id": "row1", {"sentiment": {"answer": "POSITIVE", "confidence": 0.7}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.7}}})")},
        {.name = "RepairsCommaInsteadOfColon",
         .input = R"({"row1", {"sentiment": {"answer": "POSITIVE", "confidence": 0.6}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "POSITIVE", "confidence": 0.6}}})")},
        {.name = "TotalFailureReturnsEmptyObject", .input = "not json at all, nor anything resembling it", .expected = nlohmann::json::object()},
    }),
    [](const ::testing::TestParamInfo<ParseLlmJsonCase>& info) { return info.param.name; });

TEST_P(ParseLlmJsonTest, ParsesAccordingToFallbackChain)
{
    EXPECT_EQ(parseLlmJson(GetParam().input), GetParam().expected);
}

namespace
{

struct NormalizeAnswerCase
{
    std::string name;
    std::string answer;
    std::optional<std::vector<std::string>> outputValues;
    std::string defaultValue;
    std::string expected;
};

}

class NormalizeAnswerTest : public ::testing::TestWithParam<NormalizeAnswerCase>
{
};

/// Table-driven over `_normalize_answer`'s cascade (llm_operator.py:148-165): free text (no
/// declared values), then the four progressively looser rungs, then the default fallback.
INSTANTIATE_TEST_SUITE_P(
    Cascade,
    NormalizeAnswerTest,
    ::testing::ValuesIn(std::vector<NormalizeAnswerCase>{
        {.name = "FreeTextWhenNoOutputValuesDeclared",
         .answer = "whatever the model said",
         .outputValues = std::nullopt,
         .defaultValue = "default",
         .expected = "whatever the model said"},
        {.name = "UppercaseExactMatch",
         .answer = "positive",
         .outputValues = std::vector<std::string>{"POSITIVE", "NEGATIVE"},
         .defaultValue = "default",
         .expected = "POSITIVE"},
        {.name = "StripsParentheticalThenExactMatch",
         .answer = "POSITIVE (high confidence)",
         .outputValues = std::vector<std::string>{"POSITIVE", "NEGATIVE"},
         .defaultValue = "default",
         .expected = "POSITIVE"},
        {.name = "SubstringContainment",
         .answer = "the sentiment is clearly NEGATIVE here",
         .outputValues = std::vector<std::string>{"POSITIVE", "NEGATIVE"},
         .defaultValue = "default",
         .expected = "NEGATIVE"},
        {.name = "FuzzyMatchWithinCutoff",
         .answer = "POSITVE",
         .outputValues = std::vector<std::string>{"POSITIVE", "NEGATIVE"},
         .defaultValue = "default",
         .expected = "POSITIVE"},
        {.name = "FallsBackToDefaultBelowCutoff",
         .answer = "completely unrelated text",
         .outputValues = std::vector<std::string>{"POSITIVE", "NEGATIVE"},
         .defaultValue = "default",
         .expected = "default"},
    }),
    [](const ::testing::TestParamInfo<NormalizeAnswerCase>& info) { return info.param.name; });

TEST_P(NormalizeAnswerTest, NormalizesAccordingToCascade)
{
    EXPECT_EQ(normalizeAnswer(GetParam().answer, GetParam().outputValues, GetParam().defaultValue), GetParam().expected);
}

}
