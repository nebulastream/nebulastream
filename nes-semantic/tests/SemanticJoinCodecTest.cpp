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

#include <SemanticJoinCodec.hpp>

#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

SemanticModelConfig joinConfig(std::string datasetPrompt = {})
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "llama3.1:8b";
    config.datasetPrompt = std::move(datasetPrompt);
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::FILTER,
        .prompt = "Both reviews talk about the same movie",
        .outputColumn = {},
        .outputValues = {},
        .defaultValue = {}}};
    return config;
}

/// The base sysprompt `Llm_operator` sends ahead of every flat-envelope prompt.
constexpr std::string_view ReferenceSysprompt
    = "You are a helpful AI assistant for semantic data operations.\n"
      "Always respond in JSON format.\n"
      "The response must have one key for each input row (_llm_call_id), and the value must be a JSON object with the answer and a "
      "confidence score between 0 and 1.\n"
      "Example output: {\n"
      "  \"row1\": {\"answer\": true, \"confidence\": 0.95},\n"
      "  \"row2\": {\"answer\": false, \"confidence\": 0.80}\n"
      "}\n"
      "Structure of the prompt:\n"
      "1. Operator prompt: The operator-specific instruction.\n"
      "2. User instructions: The user’s task or question.\n"
      "3. Dataset context (optional): background/domain description of the input dataset.\n"
      "4. Data: Input data, always in JSON format. Each data item (row) is a JSON object and always includes a unique row id field "
      "named _llm_call_id.\n"
      "Input data example: [ {\"_llm_call_id\": \"row1\", ...}, {\"_llm_call_id\": \"row2\", ...} ]\n"
      "Do not add explanations.\n";

constexpr std::string_view ReferenceOperatorPrompt
    = "Evaluate the following pairs (left,right) with this user instruction: Both reviews talk about the same movie Return JSON only, "
      "keyed by each pair's row id as instructed above, with {\"answer\": true/false, \"confidence\": float} as the value.\n";

std::vector<JoinPairPayload> twoPairs()
{
    return {JoinPairPayload{.pairId = "row1", .left = "a", .right = "b"}, JoinPairPayload{.pairId = "row2", .left = "c", .right = "d"}};
}

}

/// The expected prompts were produced by the Python reference itself: `SEMJoinOperator._process_rows` of llm_operator, with
/// `_call_llm` replaced by a stub that records the prompt. Escaped quote and non-ASCII character pin json.dumps' escaping.
TEST(SemanticJoinCodecTest, PromptMatchesPythonReference)
{
    const SemanticJoinCodec codec{joinConfig()};
    const std::vector pairs{JoinPairPayload{.pairId = "row1", .left = "Great movie", .right = "Terrible \"plot\" ü"}};
    EXPECT_EQ(
        codec.buildPrompt(pairs),
        fmt::format(
            "{}{}{}",
            ReferenceSysprompt,
            ReferenceOperatorPrompt,
            R"(Data: {"row1": {"left": "Great movie", "right": "Terrible \"plot\" \u00fc"}})"));
}

TEST(SemanticJoinCodecTest, DatasetContextIsGivenForBothSides)
{
    const SemanticJoinCodec codec{joinConfig("Movie reviews from Rotten Tomatoes")};
    const std::vector pairs{JoinPairPayload{.pairId = "row1", .left = "a", .right = "b"}};
    EXPECT_EQ(
        codec.buildPrompt(pairs),
        fmt::format(
            "{}{}{}",
            ReferenceSysprompt,
            ReferenceOperatorPrompt,
            "Left dataset context: Movie reviews from Rotten Tomatoes\n"
            "Right dataset context: Movie reviews from Rotten Tomatoes\n"
            R"(Data: {"row1": {"left": "a", "right": "b"}})"));
}

TEST(SemanticJoinCodecTest, SeveralPairsShareOnePrompt)
{
    const SemanticJoinCodec codec{joinConfig()};
    EXPECT_TRUE(
        codec.buildPrompt(twoPairs()).ends_with(R"(Data: {"row1": {"left": "a", "right": "b"}, "row2": {"left": "c", "right": "d"}})"));
}

TEST(SemanticJoinCodecTest, VerdictPerPair)
{
    const SemanticJoinCodec codec{joinConfig()};
    EXPECT_EQ(
        codec.parse(R"({"row1": {"answer": true, "confidence": 0.9}, "row2": {"answer": false, "confidence": 0.8}})", twoPairs()),
        (std::vector{true, false}));
    EXPECT_EQ(codec.parse(R"({"row2": {"answer": true, "confidence": 0.9}})", twoPairs()), (std::vector{false, true}));
}

/// The reference accepts only `answer is True`, so the truthy strings SEM_FILTER takes are no match here.
TEST(SemanticJoinCodecTest, OnlyBooleanTrueMatches)
{
    const SemanticJoinCodec codec{joinConfig()};
    for (const auto* const response :
         {R"({"row1": {"answer": "true"}})", R"({"row1": {"answer": 1}})", R"({"row1": true})", R"({"row1": {"confidence": 1.0}})"})
    {
        EXPECT_EQ(codec.parse(response, twoPairs()), (std::vector{false, false})) << response;
    }
}

TEST(SemanticJoinCodecTest, UnparseableOrFencedResponse)
{
    const SemanticJoinCodec codec{joinConfig()};
    EXPECT_EQ(codec.parse("I cannot compare these.", twoPairs()), (std::vector{false, false}));
    EXPECT_EQ(codec.parse("```json\n{\"row1\": {\"answer\": true, \"confidence\": 1}}\n```", twoPairs()), (std::vector{true, false}));
}

}
