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

#include <SemanticMapCodec.hpp>

#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <ResponseParsing.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

SemanticModelConfig sentimentConfig(std::vector<std::string> outputValues = {})
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "llama3.1:8b";
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Classify the sentiment",
        .outputColumn = "SENTIMENT",
        .outputValues = std::move(outputValues),
        .defaultValue = "UNKNOWN"}};
    return config;
}

std::vector<RowPayload> oneRow(std::vector<std::pair<std::string, std::string>> fields)
{
    return {RowPayload{.rowId = "row1", .fields = std::move(fields)}};
}

std::string answerOf(const SemanticMapCodec& codec, const std::string& response)
{
    const auto rows = oneRow({{"REVIEWTEXT", "whatever"}});
    const auto answers = codec.parse(response, rows);
    EXPECT_EQ(answers.size(), 1);
    EXPECT_EQ(answers.front().size(), 1);
    return answers.front().front();
}

}

/// The expected text was produced by the Python reference itself (`_build_fused_sysprompt`,
/// `_build_fused_operator_prompt`, `_dataset_context_block` and `json.dumps` of the payload), so this
/// pins byte-for-byte parity including json.dumps' separators and its \u escapes for non-ASCII text.
TEST(SemanticMapCodecTest, SpaceJoinedPromptMatchesPythonReference)
{
    auto config = sentimentConfig();
    config.datasetPrompt = "Movie reviews";
    const SemanticMapCodec codec{config};

    const auto prompt = codec.buildPrompt(oneRow({{"REVIEWTEXT", "Café \"great\"\n"}, {"TITLE", "\U0001F600 ok"}}));

    EXPECT_EQ(
        prompt,
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name and the value is a JSON "
        "object with 'answer' and 'confidence' (0-1).\n"
        "Output fields: \"SENTIMENT\"\n"
        "Example output: {\"row1\": {\"SENTIMENT\": {\"answer\": \"...\", \"confidence\": 0.9}}}\n"
        "Do not add explanations.\n"
        "Apply the following 1 operation to each row:\n"
        "  1. MAP: Classify the sentiment → output field: \"SENTIMENT\"\n"
        "Dataset context: Movie reviews\n"
        "Data: {\"row1\": \"Caf\\u00e9 \\\"great\\\"\\n \\ud83d\\ude00 ok\"}");
}

TEST(SemanticMapCodecTest, OmitsDatasetContextWhenUnset)
{
    const SemanticMapCodec codec{sentimentConfig()};
    const auto prompt = codec.buildPrompt(oneRow({{"REVIEWTEXT", "fine"}}));
    EXPECT_EQ(prompt.find("Dataset context"), std::string::npos);
    EXPECT_TRUE(
        prompt.ends_with("to each row:\n  1. MAP: Classify the sentiment → output field: \"SENTIMENT\"\nData: {\"row1\": \"fine\"}"))
        << prompt;
}

TEST(SemanticMapCodecTest, JsonObjectPayloadKeepsFieldNames)
{
    auto config = sentimentConfig();
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    const SemanticMapCodec codec{config};

    const auto prompt = codec.buildPrompt(oneRow({{"REVIEWTEXT", "a\tb"}, {"TITLE", "x"}}));

    /// Also produced by json.dumps: nested objects use the same separators.
    EXPECT_TRUE(prompt.ends_with(R"(Data: {"row1": {"REVIEWTEXT": "a\tb", "TITLE": "x"}})")) << prompt;
}

TEST(SemanticMapCodecTest, ParsesAnswerAndIgnoresConfidence)
{
    const SemanticMapCodec codec{sentimentConfig()};
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"answer": "loved it", "confidence": 0.2}}})"), "loved it");
}

/// Real models regularly lower-case the requested keys; the canonical upper-case field name must
/// still find them instead of default-filling every row.
TEST(SemanticMapCodecTest, OutputFieldLookupIsCaseInsensitive)
{
    const SemanticMapCodec codec{sentimentConfig()};
    EXPECT_EQ(answerOf(codec, R"({"row1": {"sentiment": {"answer": "loved it"}}})"), "loved it");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"Sentiment": {"answer": "meh"}}})"), "meh");
}

TEST(SemanticMapCodecTest, MissingPiecesFallBackToDefault)
{
    const SemanticMapCodec codec{sentimentConfig()};
    EXPECT_EQ(answerOf(codec, "not json at all"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, R"({"row2": {"SENTIMENT": {"answer": "x"}}})"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"OTHER": {"answer": "x"}}})"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": "POSITIVE"}})"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"confidence": 1.0}}})"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, R"(["row1"])"), "UNKNOWN");
    EXPECT_EQ(answerOf(codec, ""), "UNKNOWN");
}

TEST(SemanticMapCodecTest, NonStringAnswersAreStringifiedLikePython)
{
    const SemanticMapCodec codec{sentimentConfig()};
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"answer": 4}}})"), "4");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"answer": true}}})"), "True");
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"answer": null}}})"), "None");
}

TEST(SemanticMapCodecTest, NormalizesAgainstDeclaredValues)
{
    const SemanticMapCodec codec{sentimentConfig({"POSITIVE", "NEGATIVE"})};
    EXPECT_EQ(answerOf(codec, "```json\n{\"row1\": {\"sentiment\": {\"answer\": \"negative\", \"confidence\": 0.5}}}\n```"), "NEGATIVE");
    /// No fuzzy rung: a misspelt label is not a declared value.
    EXPECT_EQ(answerOf(codec, R"({"row1": {"SENTIMENT": {"answer": "POSITVE"}}})"), "UNKNOWN");
}

TEST(SemanticMapCodecTest, ParsesEveryRow)
{
    const SemanticMapCodec codec{sentimentConfig()};
    const std::vector<RowPayload> rows{
        RowPayload{.rowId = "row1", .fields = {{"REVIEWTEXT", "a"}}}, RowPayload{.rowId = "row2", .fields = {{"REVIEWTEXT", "b"}}}};
    const auto answers = codec.parse(R"({"row2": {"SENTIMENT": {"answer": "second"}}, "row1": {"SENTIMENT": {"answer": "first"}}})", rows);
    ASSERT_EQ(answers.size(), 2);
    EXPECT_EQ(answers[0].front(), "first");
    EXPECT_EQ(answers[1].front(), "second");
    EXPECT_TRUE(codec.buildPrompt(rows).ends_with(R"(Data: {"row1": "a", "row2": "b"})"));
}

namespace
{
struct ParseCase
{
    std::string name;
    std::string input;
    nlohmann::json expected;
};
}

class ParseLlmJsonTest : public ::testing::TestWithParam<ParseCase>
{
};

/// One case per rung of the reference's `_parse_llm_json`; each gives the earlier rungs nothing to match.
INSTANTIATE_TEST_SUITE_P(
    Cascade,
    ParseLlmJsonTest,
    ::testing::ValuesIn(std::vector<ParseCase>{
        {.name = "RawJson",
         .input = R"({"row1": {"s": {"answer": "POSITIVE", "confidence": 0.9}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "POSITIVE", "confidence": 0.9}}})")},
        {.name = "CodeFenceWithLanguageTag",
         .input = "```json\n{\"row1\": {\"s\": {\"answer\": \"NEGATIVE\"}}}\n```",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "NEGATIVE"}}})")},
        {.name = "BareCodeFence",
         .input = "Here you go:\n```\n{\"row1\": {\"s\": {\"answer\": \"NEGATIVE\"}}}\n```\nanything else?",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "NEGATIVE"}}})")},
        {.name = "OutermostObjectInProse",
         .input = R"(Sure: {"row1": {"s": {"answer": "POSITIVE"}}} Hope that helps!)",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "POSITIVE"}}})")},
        {.name = "RepairsStrayLlmCallIdKey",
         .input = R"({"_llm_call_id": "row1", {"s": {"answer": "POSITIVE"}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "POSITIVE"}}})")},
        {.name = "RepairsCommaInsteadOfColon",
         .input = R"({"row1", {"s": {"answer": "POSITIVE"}}})",
         .expected = nlohmann::json::parse(R"({"row1": {"s": {"answer": "POSITIVE"}}})")},
        {.name = "TotalFailure", .input = "no json here", .expected = nlohmann::json::object()},
    }),
    [](const ::testing::TestParamInfo<ParseCase>& info) { return info.param.name; });

TEST_P(ParseLlmJsonTest, FollowsCascade)
{
    EXPECT_EQ(parseLlmJson(GetParam().input), GetParam().expected);
}

/// The greedy-object rung must not recurse per character (std::regex does, and overflows the stack).
TEST(ParseLlmJsonLargeTest, HandlesLargeResponses)
{
    const std::string padding(1U << 20U, 'x');
    const auto response = "prose " + padding + R"( {"row1": {"s": {"answer": ")" + padding + R"("}}} more prose)";
    const auto parsed = parseLlmJson(response);
    ASSERT_TRUE(parsed.contains("row1"));
    EXPECT_EQ(parsed["row1"]["s"]["answer"].get<std::string>().size(), padding.size());
}

namespace
{
struct NormalizeCase
{
    std::string name;
    std::string answer;
    std::vector<std::string> outputValues;
    std::string expected;
};
}

class NormalizeAnswerTest : public ::testing::TestWithParam<NormalizeCase>
{
};

INSTANTIATE_TEST_SUITE_P(
    Cascade,
    NormalizeAnswerTest,
    ::testing::ValuesIn(std::vector<NormalizeCase>{
        {.name = "FreeText", .answer = " kept verbatim ", .outputValues = {}, .expected = " kept verbatim "},
        {.name = "CaseInsensitiveExact", .answer = " positive ", .outputValues = {"POSITIVE", "NEGATIVE"}, .expected = "POSITIVE"},
        {.name = "KeepsDeclaredSpelling", .answer = "POSITIVE", .outputValues = {"Positive", "Negative"}, .expected = "Positive"},
        {.name = "StripsParenthetical",
         .answer = "NEGATIVE (fairly sure)",
         .outputValues = {"POSITIVE", "NEGATIVE"},
         .expected = "NEGATIVE"},
        {.name = "Containment", .answer = "clearly negative overall", .outputValues = {"POSITIVE", "NEGATIVE"}, .expected = "NEGATIVE"},
        {.name = "ContainmentTieBreaksInDeclarationOrder",
         .answer = "NEGATIVE OR POSITIVE",
         .outputValues = {"POSITIVE", "NEGATIVE"},
         .expected = "POSITIVE"},
        {.name = "NoFuzzyMatch", .answer = "POSITVE", .outputValues = {"POSITIVE", "NEGATIVE"}, .expected = "DEFAULT"},
        {.name = "Default", .answer = "no idea", .outputValues = {"POSITIVE", "NEGATIVE"}, .expected = "DEFAULT"},
    }),
    [](const ::testing::TestParamInfo<NormalizeCase>& info) { return info.param.name; });

TEST_P(NormalizeAnswerTest, FollowsCascade)
{
    EXPECT_EQ(normalizeAnswer(GetParam().answer, GetParam().outputValues, "DEFAULT"), GetParam().expected);
}

}
