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

#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <SemanticImage.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

SemanticStep filterStep()
{
    return SemanticStep{
        .kind = SemanticStep::Kind::FILTER,
        .prompt = "Is this movie review positive?",
        .outputColumn = {},
        .outputValues = {},
        .defaultValue = {}};
}

SemanticStep mapStep(std::vector<std::string> values)
{
    return SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = "Which sentiment?",
        .outputColumn = "SENTIMENT",
        .outputValues = std::move(values),
        .defaultValue = "UNKNOWN"};
}

}

/// Byte for byte the prompt of llm_operator's finetune_label_token.py (build_prompts): the d1q2 instruction, then
/// ANSWER_INSTRUCTION, a blank line and "Review: <text>".
TEST(SemanticLabelAnswerTest, FilterPromptIsTheStudentsTrainingPrompt)
{
    const auto values = labelValues(filterStep());
    EXPECT_EQ(values, (std::vector<std::string>{"true", "false"}));
    EXPECT_EQ(
        buildLabelPrompt("Is this movie review positive?", values, "Review", "A joy from start to finish."),
        "Is this movie review positive? Answer only with true or false.\n\nReview: A joy from start to finish.");
}

TEST(SemanticLabelAnswerTest, MapPromptListsTheOutputValues)
{
    EXPECT_EQ(labelAnswerInstruction({"POSITIVE", "NEGATIVE"}), "Answer only with POSITIVE or NEGATIVE.");
    EXPECT_EQ(labelAnswerInstruction({"cat", "dog", "bird"}), "Answer only with cat, dog or bird.");
    const auto values = labelValues(mapStep({"cat", "dog"}));
    EXPECT_EQ(buildLabelPrompt("Which animal?", values, "Text", "meow"), "Which animal? Answer only with cat or dog.\n\nText: meow");
}

TEST(SemanticLabelAnswerTest, AnswerIsOneOfTheValuesIgnoringCaseAndDecoration)
{
    const std::vector<std::string> values{"true", "false"};
    EXPECT_EQ(parseLabelAnswer("true", values), "true");
    EXPECT_EQ(parseLabelAnswer("True", values), "true");
    EXPECT_EQ(parseLabelAnswer(" FALSE\n", values), "false");
    EXPECT_EQ(parseLabelAnswer("\"true\"", values), "true");
    EXPECT_EQ(parseLabelAnswer("`false`.", values), "false");
    EXPECT_EQ(parseLabelAnswer("True.", values), "true");
    /// Anything else is no answer: the caller writes the default, a filter drops the row.
    EXPECT_EQ(parseLabelAnswer("", values), std::nullopt);
    EXPECT_EQ(parseLabelAnswer("yes", values), std::nullopt);
    EXPECT_EQ(parseLabelAnswer("true, because it praises the cast", values), std::nullopt);
    EXPECT_EQ(parseLabelAnswer("{\"row1\": true}", values), std::nullopt);
    /// A MAP step answers with the value as declared, whatever case the model used.
    EXPECT_EQ(parseLabelAnswer("negative", {"POSITIVE", "NEGATIVE"}), "NEGATIVE");
}

TEST(SemanticLabelAnswerTest, RequestAsksForOneTokenWithThinkingOff)
{
    auto request = buildChatRequest("student", "prompt", {}, 0);
    applyLabelRequestFields(request, 0);
    EXPECT_EQ(request["max_tokens"], 1);
    EXPECT_EQ(request["chat_template_kwargs"], nlohmann::json({{"enable_thinking", false}}));
    EXPECT_EQ(request["messages"].size(), 1);
    EXPECT_EQ(request["messages"][0]["role"], "user");
    EXPECT_EQ(request["messages"][0]["content"], "prompt");
    /// LLM.MAX_TOKENS still wins, e.g. for a model that writes "True." in three tokens.
    applyLabelRequestFields(request, 4);
    EXPECT_EQ(request["max_tokens"], 4);
}

}
