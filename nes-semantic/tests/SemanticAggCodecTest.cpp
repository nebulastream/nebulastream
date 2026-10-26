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

#include <string>
#include <string_view>
#include <utility>

#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{
SemanticModelConfig aggConfig(std::string datasetPrompt = {}, std::string defaultValue = {})
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "llama3.1:8b";
    config.datasetPrompt = std::move(datasetPrompt);
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Summarize the reviews in one sentence",
        .outputColumn = "SUMMARY",
        .outputValues = {},
        .defaultValue = std::move(defaultValue)}};
    return config;
}

constexpr std::string_view ReferenceSysprompt
    = "You are a helpful AI assistant for semantic data aggregation.\n"
      "You receive a window of records and must produce a single aggregate result for the entire window.\n"
      "Respond with the aggregate result according to the aggregation instruction.\n"
      "Do not add explanations unless the instruction specifically asks for them.\n"
      "Use exactly the output format the aggregation instruction asks for (for example a JSON object); if it names none, answer "
      "in plain text. Never wrap the answer in markdown code fences (```).\n"
      "Aggregation instruction: Summarize the reviews in one sentence\n";
}

/// The expected prompts were produced by the Python reference itself: `SEMAggOperator._process_agg_payload` of
/// llm_operator, with `_call_llm` replaced by a stub that records the prompt (~/Desktop/sem-call-prototype/golden-agg).
TEST(SemanticAggCodecTest, PromptMatchesPythonReference)
{
    const SemanticAggCodec codec{aggConfig(), "TEXT"};
    EXPECT_EQ(
        codec.buildPrompt(R"(["Great movie","Terrible \"plot\" ü",""])"),
        std::string{ReferenceSysprompt} + "Window:\n" + R"([{"TEXT": "Great movie"}, {"TEXT": "Terrible \"plot\" \u00fc"}, {"TEXT": ""}])");
}

TEST(SemanticAggCodecTest, DatasetContextSitsBeforeTheWindow)
{
    const SemanticAggCodec codec{aggConfig("Movie reviews from Rotten Tomatoes"), "TEXT"};
    EXPECT_EQ(
        codec.buildPrompt(R"(["a"])"),
        std::string{ReferenceSysprompt} + "Dataset context: Movie reviews from Rotten Tomatoes\nWindow:\n" + R"([{"TEXT": "a"}])");
}

TEST(SemanticAggCodecTest, EmptyWindowIsAnEmptyList)
{
    const SemanticAggCodec codec{aggConfig(), "TEXT"};
    EXPECT_TRUE(codec.buildPrompt("[]").ends_with("Window:\n[]"));
}

TEST(SemanticAggCodecTest, RejectsAnythingButAnArrayOfStrings)
{
    const SemanticAggCodec codec{aggConfig(), "TEXT"};
    EXPECT_ANY_THROW(std::ignore = codec.buildPrompt("not json"));
    EXPECT_ANY_THROW(std::ignore = codec.buildPrompt(R"({"a": 1})"));
    EXPECT_ANY_THROW(std::ignore = codec.buildPrompt(R"(["a", 1])"));
}

/// `_strip_fence` of the reference: a whole answer in one fence is unwrapped and stripped, anything else is kept as it is.
TEST(SemanticAggCodecTest, FencedAnswerIsUnwrapped)
{
    const SemanticAggCodec codec{aggConfig(), "TEXT"};
    EXPECT_EQ(codec.parse("```json\n{\"summary\": \"ok\"}\n```"), R"({"summary": "ok"})");
    EXPECT_EQ(codec.parse("  plain answer  "), "  plain answer  ");
    EXPECT_EQ(codec.parse("text ```inline``` text"), "text ```inline``` text");
}

TEST(SemanticAggCodecTest, EmptyAnswerGivesTheDefault)
{
    const SemanticAggCodec codec{aggConfig({}, "NO SUMMARY"), "TEXT"};
    EXPECT_EQ(codec.parse(""), "NO SUMMARY");
    EXPECT_EQ(codec.parse("```\n\n```"), "NO SUMMARY");
}

}
