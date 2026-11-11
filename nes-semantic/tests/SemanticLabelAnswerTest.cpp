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

#include <cmath>
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

/// The training script's p(true): softmax over the logits of "true" and "false" at the answer position. Over logprobs of
/// one distribution that is the same number, since both share the log-partition.
TEST(SemanticLabelAnswerTest, ProbabilityIsTheSoftmaxOverTheValuesLogprobs)
{
    const std::vector<std::string> values{"true", "false"};
    const TokenLogprobs alternatives{{"true", std::log(0.6)}, {"The", std::log(0.3)}, {"false", std::log(0.05)}};
    const auto p = labelProbability(alternatives, values, "true");
    ASSERT_TRUE(p.has_value());
    EXPECT_NEAR(*p, 0.6 / 0.65, 1e-12);
    EXPECT_NEAR(*labelProbability(alternatives, values, "false"), 0.05 / 0.65, 1e-12);
    /// Spellings of a value add up: qwen2.5:0.5b (Ollama) answers "True" and lists "true" far below it.
    const TokenLogprobs capitalized{{"True", -0.82}, {"False", -0.62}, {"false", -4.86}, {"The", -5.25}, {"true", -5.49}, {" True", -8.5}};
    const double pTrue = std::exp(-0.82) + std::exp(-5.49) + std::exp(-8.5);
    const double pFalse = std::exp(-0.62) + std::exp(-4.86);
    EXPECT_NEAR(*labelProbability(capitalized, values, "true"), pTrue / (pTrue + pFalse), 1e-12);
    /// "false" not among the alternatives: it counts as 0, as when vLLM lists only the top 20 and "false" is far below.
    EXPECT_DOUBLE_EQ(*labelProbability({{"true", -0.01}, {"True", -4.6}}, values, "true"), 1.0);
    EXPECT_DOUBLE_EQ(*labelProbability({{"false", -0.2}}, values, "true"), 0.0);
    /// Neither value listed, or a target that is no value: no probability.
    EXPECT_EQ(labelProbability({{"Yes", -0.1}, {"No", -2.0}}, values, "true"), std::nullopt);
    EXPECT_EQ(labelProbability({}, values, "true"), std::nullopt);
    EXPECT_EQ(labelProbability(alternatives, values, "maybe"), std::nullopt);
    /// Very negative logprobs do not underflow to 0/0.
    EXPECT_NEAR(*labelProbability({{"true", -1000.0}, {"false", -1001.0}}, values, "true"), 1.0 / (1.0 + std::exp(-1.0)), 1e-12);
    EXPECT_EQ(formatProbability(0.6 / 0.65), "0.923077");
}

TEST(SemanticLabelAnswerTest, LogprobsOfTheFirstTokenAreReadFromTheEnvelope)
{
    /// The shape vLLM and Ollama (0.30.9, /v1/chat/completions) send.
    const auto envelope = nlohmann::json::parse(R"({"choices": [{"message": {"content": "true"}, "logprobs": {"content": [
        {"token": "true", "logprob": -0.07, "top_logprobs": [{"token": "true", "logprob": -0.07}, {"token": "True", "logprob": -2.7}]},
        {"token": "<eos>", "logprob": -0.001, "top_logprobs": []}]}}]})");
    const auto alternatives = firstTokenLogprobs(envelope);
    ASSERT_EQ(alternatives.size(), 2);
    EXPECT_EQ(alternatives[0].first, "true");
    EXPECT_DOUBLE_EQ(alternatives[0].second, -0.07);
    EXPECT_EQ(alternatives[1].first, "True");
    /// Without top_logprobs, the generated token alone; without logprobs, nothing.
    const auto tokenOnly = nlohmann::json::parse(R"({"choices": [{"logprobs": {"content": [{"token": "false", "logprob": -0.5}]}}]})");
    EXPECT_EQ(firstTokenLogprobs(tokenOnly), (TokenLogprobs{{"false", -0.5}}));
    EXPECT_TRUE(firstTokenLogprobs(nlohmann::json::parse(R"({"choices": [{"message": {"content": "true"}}]})")).empty());
    EXPECT_TRUE(firstTokenLogprobs(nlohmann::json::parse(R"({"choices": [{"logprobs": null}]})")).empty());
    EXPECT_TRUE(firstTokenLogprobs(nlohmann::json::parse("[]")).empty());
}

TEST(SemanticLabelAnswerTest, ConfidenceAsksForTheTopLogprobs)
{
    auto request = buildChatRequest("student", "prompt", {}, 0);
    applyLabelRequestFields(request, 0);
    applyLogprobRequestFields(request);
    EXPECT_EQ(request["logprobs"], true);
    EXPECT_EQ(request["top_logprobs"], LabelTopLogprobs);
}

}
