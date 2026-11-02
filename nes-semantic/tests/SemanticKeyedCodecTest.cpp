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

#include <optional>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <SemanticCallPayload.hpp>
#include <SemanticKeyedCodec.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{
namespace
{

SemanticModelConfig keyedConfig(const SemanticKeyedConfig::Kind kind, std::string prompt, std::string datasetPrompt)
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "gemma4";
    config.datasetPrompt = std::move(datasetPrompt);
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP, .prompt = std::move(prompt), .outputColumn = "STATE", .outputValues = {}, .defaultValue = "n/a"}};
    config.keyed.emplace();
    config.keyed->kind = kind;
    return config;
}

SemanticWindow window(std::string id, std::string description, std::vector<std::string> examples)
{
    SemanticWindow result{.id = std::move(id), .description = std::move(description), .start = "", .rows = examples.size(), .lastRow = examples.back(), .recentExamples = {}};
    result.recentExamples.assign(examples.begin(), examples.end());
    return result;
}

}

/// The expected prompts were produced by the Python reference itself: `SEMScanOperator._build_prompt` and
/// `SEMWindowOperator._evaluate_windows_batch` of llm_operator, with `_call_llm` replaced by a stub that records the prompt
/// (~/Desktop/sem-call-prototype/golden-keyed).
TEST(SemanticKeyedCodecTest, ScanPromptsMatchPythonReference)
{
    auto config = keyedConfig(SemanticKeyedConfig::Kind::SCAN, "Fold the review", "");
    config.keyed->initPrompt = "Start a digest";
    EXPECT_EQ(SemanticScanCodec{config}.buildPrompt("Great \"movie\" \xc3\xbc", std::nullopt),
      "You are a semantic scan assistant computing a running per-key accumulator.\n"
      "This is the FIRST tuple seen for this key -- there is no prior state, so establish the initial accumulator from this tuple alone.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for the input row (_llm_call_id), and the value must be a JSON object with 'answer' (the computed accumulator/result, as a string -- encode structured content as a JSON string if needed) and 'confidence' (a float between 0 and 1).\n"
      "Example output: {\"row1\": {\"answer\": \"...\", \"confidence\": 0.9}}\n"
      "Do not add explanations.\n"
      "Instruction: Start a digest\n"
      "Data: {\"row1\": \"Great \\\"movie\\\" \\u00fc\"}");
    config.datasetPrompt = "Movie reviews";
    EXPECT_EQ(SemanticScanCodec{config}.buildPrompt("classic", std::string{"scary|dark"}),
      "You are a semantic scan assistant computing a running per-key accumulator.\n"
      "A prior accumulator for this key is provided below -- fold the new tuple into it to produce the updated accumulator.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for the input row (_llm_call_id), and the value must be a JSON object with 'answer' (the updated accumulator/result, as a string -- encode structured content as a JSON string if needed) and 'confidence' (a float between 0 and 1).\n"
      "Example output: {\"row1\": {\"answer\": \"...\", \"confidence\": 0.9}}\n"
      "Do not add explanations.\n"
      "Instruction: Fold the review\n"
      "Dataset context: Movie reviews\n"
      "\n"
      "Prior state for this key: \"scary|dark\"\n"
      "Data: {\"row1\": \"classic\"}");
}

TEST(SemanticKeyedCodecTest, ScanAnswerFallsBackToTheOnlyEntryAndFailsOnNone)
{
    const SemanticScanCodec codec{keyedConfig(SemanticKeyedConfig::Kind::SCAN, "Fold", "")};
    const auto exact = codec.parse(R"({"row1": {"answer": "a|b", "confidence": 0.8}})");
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(exact->state, "a|b");
    EXPECT_EQ(exact->confidence, "0.8");
    const auto mangled = codec.parse(R"({"_llm_call_id": {"answer": "x", "confidence": 1.0}})");
    ASSERT_TRUE(mangled.has_value());
    EXPECT_EQ(mangled->state, "x");
    EXPECT_FALSE(codec.parse("no json at all").has_value());
    EXPECT_FALSE(codec.parse(R"({"row7": {"answer": "x"}, "row8": {"answer": "y"}})").has_value());
}

TEST(SemanticKeyedCodecTest, WindowPromptsMatchPythonReference)
{
    auto config = keyedConfig(SemanticKeyedConfig::Kind::WINDOW, "Group posts by topic", "Posts");
    const auto w1 = window("w1", "sports", {"sports: a", "sports: b"});
    const auto w2 = window("w2", "", {"cooking: \xc3\xbc"});
    EXPECT_EQ(SemanticWindowCodec{config}.buildPrompt("sports: final", {&w1, &w2}),
      "You are a semantic stream windowing assistant.\n"
      "Given a new tuple and the contexts of several active semantic windows, score how well the tuple fits each window.\n"
      "Respond only in JSON keyed by window_id. Each value must have these three keys:\n"
      "  continuity_score: float 0-1 \xe2\x80\x94"" how well this tuple belongs to that window (1 = perfect fit, 0 = completely unrelated).\n"
      "  window_description: updated concise description of that window's semantic theme assuming this tuple were added to it.\n"
      "  new_window_description: concise description of a brand-new window that would start with this tuple (used only if the tuple does not fit that window).\n"
      "Do not add explanations outside the JSON.\n"
      "\n"
      "Threshold: 0.7\n"
      "Group posts by topic\n"
      "\n"
      "Dataset context: Posts\n"
      "Windows to score:\n"
      "{\"w1\": {\"description\": \"sports\", \"recent_examples\": [\"sports: a\", \"sports: b\"]}, \"w2\": {\"description\": \"\", \"recent_examples\": [\"cooking: \\u00fc\"]}}\n"
      "\n"
      "New tuple: sports: final");
    auto pairwise = keyedConfig(SemanticKeyedConfig::Kind::WINDOW, "Group", "");
    pairwise.keyed->strategy = "pairwise";
    pairwise.keyed->threshold = 1.0;
    const auto only = window("w1", "", {"x: y"});
    EXPECT_EQ(SemanticWindowCodec{pairwise}.buildPrompt("z", {&only}),
      "You are a semantic stream windowing assistant.\n"
      "Given a new tuple and the contexts of several active semantic windows, score how well the tuple fits each window.\n"
      "Respond only in JSON keyed by window_id. Each value must have these three keys:\n"
      "  continuity_score: float 0-1 \xe2\x80\x94"" how well this tuple belongs to that window (1 = perfect fit, 0 = completely unrelated).\n"
      "  window_description: updated concise description of that window's semantic theme assuming this tuple were added to it.\n"
      "  new_window_description: concise description of a brand-new window that would start with this tuple (used only if the tuple does not fit that window).\n"
      "Do not add explanations outside the JSON.\n"
      "\n"
      "Threshold: 1.0\n"
      "Group\n"
      "\n"
      "Windows to score:\n"
      "{\"w1\": {\"description\": \"\", \"most_recent_tuple\": \"x: y\"}}\n"
      "\n"
      "New tuple: z");
}

TEST(SemanticKeyedCodecTest, WindowAssignmentFollowsTheReference)
{
    auto config = keyedConfig(SemanticKeyedConfig::Kind::WINDOW, "Group", "");
    config.keyed->maxActiveWindows = 2;
    const SemanticWindowCodec codec{config};
    SemanticWindowState state;
    EXPECT_EQ(codec.assign(state, "a", "t1", {}), (std::vector<std::string>{"w1", "", "t1", "1.0", "true"}));
    /// Below the threshold: a new window with the best window's new description.
    EXPECT_EQ(
        codec.assign(state, "b", "t2", {{.continuity = 0.2, .windowDescription = "A", .newWindowDescription = "B"}}),
        (std::vector<std::string>{"w2", "B", "t2", "0.2", "true"}));
    /// The first of equal best scores wins.
    EXPECT_EQ(
        codec.assign(
            state,
            "a2",
            "t3",
            {{.continuity = 0.9, .windowDescription = "A2", .newWindowDescription = "-"},
             {.continuity = 0.9, .windowDescription = "B2", .newWindowDescription = "-"}}),
        (std::vector<std::string>{"w1", "A2", "t1", "0.9", "false"}));
    /// A third window closes the oldest.
    EXPECT_EQ(
        codec.assign(
            state,
            "c",
            "t4",
            {{.continuity = 0.1, .windowDescription = "-", .newWindowDescription = "C"},
             {.continuity = 0.3, .windowDescription = "-", .newWindowDescription = "C3"}}),
        (std::vector<std::string>{"w3", "C3", "t4", "0.3", "true"}));
    EXPECT_EQ(state.active, (std::vector<std::string>{"w2", "w3"}));
    EXPECT_EQ(state.windows.at("w1").recentExamples, (std::deque<std::string>{"a", "a2"}));
}

TEST(SemanticKeyedCodecTest, WindowScoresDefaultForWindowsTheAnswerLeavesOut)
{
    const SemanticWindowCodec codec{keyedConfig(SemanticKeyedConfig::Kind::WINDOW, "Group", "")};
    const auto w1 = window("w1", "old", {"x"});
    const auto w2 = window("w2", "other", {"y"});
    const auto scores = codec.parse(R"({"w2": {"continuity_score": "0.75", "window_description": "new"}})", "text", {&w1, &w2});
    ASSERT_EQ(scores.size(), 2);
    EXPECT_EQ(scores[0].continuity, 0.0);
    EXPECT_EQ(scores[0].windowDescription, "old");
    EXPECT_EQ(scores[0].newWindowDescription, "text");
    EXPECT_EQ(scores[1].continuity, 0.75);
    EXPECT_EQ(scores[1].windowDescription, "new");
}

TEST(SemanticKeyedCodecTest, TruncatesByCodePointsAsPythonSlices)
{
    EXPECT_EQ(detail::truncateCodePoints("\xc3\xbc\xc3\xbc\xc3\xbc", 2), "\xc3\xbc\xc3\xbc");
    EXPECT_EQ(detail::truncateCodePoints("abc", 5), "abc");
}

TEST(SemanticKeyedCodecTest, PayloadKeepsTheKeyedConfiguration)
{
    SemanticKeyedConfig keyed;
    keyed.kind = SemanticKeyedConfig::Kind::WINDOW;
    keyed.keyBy = "author";
    keyed.threshold = 0.65;
    keyed.maxActiveWindows = 3;
    EXPECT_EQ(decodeSemanticKeyedConfig(encodeSemanticKeyedConfig(keyed)), keyed);
}

}
