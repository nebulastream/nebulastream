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

#include <SemanticFilterCodec.hpp>

#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <SemanticModelCatalog.hpp>
#include <SemanticRowPayload.hpp>

namespace NES
{

namespace
{

SemanticStep filterStep(std::string prompt)
{
    return SemanticStep{
        .kind = SemanticStep::Kind::FILTER, .prompt = std::move(prompt), .outputColumn = {}, .outputValues = {}, .defaultValue = {}};
}

SemanticStep mapStep(std::string prompt, std::string column, std::vector<std::string> outputValues = {}, std::string defaultValue = {})
{
    return SemanticStep{
        .kind = SemanticStep::Kind::MAP,
        .prompt = std::move(prompt),
        .outputColumn = std::move(column),
        .outputValues = std::move(outputValues),
        .defaultValue = std::move(defaultValue)};
}

SemanticModelConfig configWith(std::vector<SemanticStep> steps)
{
    SemanticModelConfig config;
    config.endpoint = "http://localhost:11434/v1";
    config.modelName = "llama3.1:8b";
    config.steps = std::move(steps);
    return config;
}

/// The rows every golden prompt below was generated with: an escaped quote and a non-ASCII
/// character, so json.dumps' escaping is pinned as well.
std::vector<RowPayload> referenceRows()
{
    return {
        RowPayload{.rowId = "row1", .fields = {{"REVIEWTEXT", "Great movie"}}},
        RowPayload{.rowId = "row2", .fields = {{"REVIEWTEXT", "Terrible \"plot\" ü"}}}};
}

std::vector<RowPayload> twoRows()
{
    return {RowPayload{.rowId = "row1", .fields = {{"REVIEWTEXT", "a"}}}, RowPayload{.rowId = "row2", .fields = {{"REVIEWTEXT", "b"}}}};
}

/// Which of `twoRows()` pass under a single-filter codec for the given response.
std::vector<bool> passedRows(const std::string& response)
{
    const SemanticFilterCodec codec{configWith({filterStep("The review is positive")})};
    std::vector<bool> passed;
    for (const auto& result : codec.parse(response, twoRows()))
    {
        passed.push_back(result.passed);
        EXPECT_TRUE(result.mapAnswers.empty());
    }
    return passed;
}

bool singleVerdict(const std::string& answer)
{
    return passedRows(R"({"row1": {"answer": )" + answer + R"(, "confidence": 0.9}})").front();
}

}

/// Every expected prompt in this file was produced by the Python reference itself, by running
/// `SEMFilterOperator._process_rows` (and `fuse`) with `_call_llm` replaced by a recorder — so this
/// pins byte-for-byte parity with `_filter_with_llm` and `_process_fused_steps`.
TEST(SemanticFilterCodecTest, SingleFilterPromptMatchesPythonReference)
{
    const SemanticFilterCodec codec{configWith({filterStep("The review is positive")})};
    EXPECT_EQ(
        codec.buildPrompt(referenceRows()),
        "You are a helpful AI assistant for semantic data operations.\n"
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
        "4. Data: Input data, always in JSON format. Each data item (row) is a JSON object and always includes a unique row id field named "
        "_llm_call_id.\n"
        "Input data example: [ {\"_llm_call_id\": \"row1\", ...}, {\"_llm_call_id\": \"row2\", ...} ]\n"
        "Do not add explanations.\n"
        "Filter rows: The review is positive\n"
        "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}");
}

TEST(SemanticFilterCodecTest, DatasetContextSitsBetweenOperatorPromptAndData)
{
    auto config = configWith({filterStep("The review is positive")});
    config.datasetPrompt = "Movie reviews";
    const SemanticFilterCodec codec{config};
    const auto prompt = codec.buildPrompt(referenceRows());
    EXPECT_TRUE(prompt.ends_with("Filter rows: The review is positive\n"
                                 "Dataset context: Movie reviews\n"
                                 "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}"))
        << prompt;
}

/// `_rebuild_filter_operator_prompt`: fused filters stay in the flat layout, with one condition per line.
TEST(SemanticFilterCodecTest, FusedFiltersPromptMatchesPythonReference)
{
    const SemanticFilterCodec codec{configWith({filterStep("The review is positive"), filterStep("The review mentions acting")})};
    EXPECT_EQ(
        codec.buildPrompt(referenceRows()),
        "You are a helpful AI assistant for semantic data operations.\n"
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
        "4. Data: Input data, always in JSON format. Each data item (row) is a JSON object and always includes a unique row id field named "
        "_llm_call_id.\n"
        "Input data example: [ {\"_llm_call_id\": \"row1\", ...}, {\"_llm_call_id\": \"row2\", ...} ]\n"
        "Do not add explanations.\n"
        "Filter rows that satisfy ALL of the following conditions:\n"
        "  1. The review is positive\n"
        "  2. The review mentions acting\n"
        "A row passes only if ALL conditions are true.\n"
        "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}");
}

/// A map fused with a downstream filter: the fused layout, with the filter's verdict under `__filter_0`.
TEST(SemanticFilterCodecTest, MapThenFilterPromptMatchesPythonReference)
{
    const SemanticFilterCodec codec{configWith({mapStep("Classify the sentiment", "SENTIMENT"), filterStep("The review is positive")})};
    EXPECT_EQ(
        codec.buildPrompt(referenceRows()),
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name and the value is a JSON object "
        "with 'answer' and 'confidence' (0-1).\n"
        "Output fields: \"SENTIMENT\", \"__filter_0\"\n"
        "Example output: {\"row1\": {\"SENTIMENT\": {\"answer\": \"...\", \"confidence\": 0.9}, \"__filter_0\": {\"answer\": true, "
        "\"confidence\": 0.9}}}\n"
        "Do not add explanations.\n"
        "Apply the following 2 operations to each row:\n"
        "  1. MAP: Classify the sentiment → output field: \"SENTIMENT\"\n"
        "  2. FILTER: The review is positive → output field: \"__filter_0\" (true/false)\n"
        "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}");
}

TEST(SemanticFilterCodecTest, FilterThenMapPromptMatchesPythonReference)
{
    const SemanticFilterCodec codec{configWith({filterStep("The review is positive"), mapStep("Classify the sentiment", "SENTIMENT")})};
    EXPECT_EQ(
        codec.buildPrompt(referenceRows()),
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name and the value is a JSON object "
        "with 'answer' and 'confidence' (0-1).\n"
        "Output fields: \"__filter_0\", \"SENTIMENT\"\n"
        "Example output: {\"row1\": {\"__filter_0\": {\"answer\": true, \"confidence\": 0.9}, \"SENTIMENT\": {\"answer\": \"...\", "
        "\"confidence\": 0.9}}}\n"
        "Do not add explanations.\n"
        "Apply the following 2 operations to each row:\n"
        "  1. FILTER: The review is positive → output field: \"__filter_0\" (true/false)\n"
        "  2. MAP: Classify the sentiment → output field: \"SENTIMENT\"\n"
        "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}");
}

/// Filters are numbered among themselves, whatever the stored output column says.
TEST(SemanticFilterCodecTest, FilterFilterMapPromptMatchesPythonReference)
{
    auto first = filterStep("The review is positive");
    first.outputColumn = "ignored";
    const SemanticFilterCodec codec{
        configWith({std::move(first), filterStep("The review mentions acting"), mapStep("Classify the sentiment", "SENTIMENT")})};
    EXPECT_EQ(
        codec.buildPrompt(referenceRows()),
        "You are a helpful AI assistant for semantic data operations.\n"
        "Always respond in JSON format.\n"
        "For each input row (_llm_call_id), return a JSON object where each key is an output field name and the value is a JSON object "
        "with 'answer' and 'confidence' (0-1).\n"
        "Output fields: \"__filter_0\", \"__filter_1\", \"SENTIMENT\"\n"
        "Example output: {\"row1\": {\"__filter_0\": {\"answer\": true, \"confidence\": 0.9}, \"__filter_1\": {\"answer\": true, "
        "\"confidence\": 0.9}, \"SENTIMENT\": {\"answer\": \"...\", \"confidence\": 0.9}}}\n"
        "Do not add explanations.\n"
        "Apply the following 3 operations to each row:\n"
        "  1. FILTER: The review is positive → output field: \"__filter_0\" (true/false)\n"
        "  2. FILTER: The review mentions acting → output field: \"__filter_1\" (true/false)\n"
        "  3. MAP: Classify the sentiment → output field: \"SENTIMENT\"\n"
        "Data: {\"row1\": \"Great movie\", \"row2\": \"Terrible \\\"plot\\\" \\u00fc\"}");
}

TEST(SemanticFilterCodecTest, JsonObjectPayloadKeepsFieldNames)
{
    auto config = configWith({filterStep("The review is positive")});
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    const SemanticFilterCodec codec{config};
    const std::vector rows{RowPayload{.rowId = "row1", .fields = {{"REVIEWTEXT", "a"}, {"TITLE", "x"}}}};
    const auto prompt = codec.buildPrompt(rows);
    EXPECT_TRUE(prompt.ends_with(R"(Data: {"row1": {"REVIEWTEXT": "a", "TITLE": "x"}})")) << prompt;
}

TEST(SemanticFilterCodecTest, FlatEnvelopeKeepsAffirmedRowsOnly)
{
    EXPECT_EQ(
        passedRows(R"({"row1": {"answer": true, "confidence": 0.9}, "row2": {"answer": false, "confidence": 0.8}})"),
        (std::vector{true, false}));
}

/// The reference falls back to `bool(result)` when a row's value is not an object.
TEST(SemanticFilterCodecTest, BareVerdictWithoutEnvelopeCounts)
{
    EXPECT_EQ(passedRows(R"({"row1": true, "row2": "no"})"), (std::vector{true, false}));
}

TEST(SemanticFilterCodecTest, TruthinessMatrix)
{
    EXPECT_TRUE(singleVerdict("true"));
    EXPECT_TRUE(singleVerdict(R"("true")"));
    EXPECT_TRUE(singleVerdict(R"("TRUE")"));
    EXPECT_TRUE(singleVerdict(R"(" Yes ")"));
    EXPECT_TRUE(singleVerdict("1"));
    EXPECT_TRUE(singleVerdict("0.5"));
    EXPECT_TRUE(singleVerdict("-2"));

    EXPECT_FALSE(singleVerdict("false"));
    EXPECT_FALSE(singleVerdict("0"));
    EXPECT_FALSE(singleVerdict("0.0"));
    EXPECT_FALSE(singleVerdict("null"));
    EXPECT_FALSE(singleVerdict(R"("")"));
    EXPECT_FALSE(singleVerdict(R"("no")"));
    EXPECT_FALSE(singleVerdict(R"("maybe")"));
    EXPECT_FALSE(singleVerdict("[true]"));
    EXPECT_FALSE(singleVerdict(R"({"answer": true})"));
    /// The one deliberate deviation from the reference: Python's `bool("false")` is True.
    EXPECT_FALSE(singleVerdict(R"("false")"));
    EXPECT_FALSE(singleVerdict(R"("False")"));
}

TEST(SemanticFilterCodecTest, MissingRowOrAnswerDropsTheRow)
{
    EXPECT_EQ(passedRows(R"({"row1": {"answer": true}})"), (std::vector{true, false}));
    EXPECT_EQ(passedRows(R"({"row1": {"confidence": 0.9}, "row2": {}})"), (std::vector{false, false}));
}

TEST(SemanticFilterCodecTest, UnparseableResponseDropsEveryRow)
{
    EXPECT_EQ(passedRows("I am sorry, but I cannot classify these rows."), (std::vector{false, false}));
    EXPECT_EQ(passedRows(""), (std::vector{false, false}));
}

/// The parse cascade is shared with SEM_MAP: prose around a fenced block still decodes.
TEST(SemanticFilterCodecTest, FencedResponseIsDecoded)
{
    EXPECT_EQ(passedRows("Sure!\n```json\n{\"row1\": {\"answer\": true}, \"row2\": {\"answer\": true}}\n```"), (std::vector{true, true}));
}

/// Fused filters still get one verdict per row: the prompt asks for all conditions at once.
TEST(SemanticFilterCodecTest, FusedFiltersReadTheFlatEnvelope)
{
    const SemanticFilterCodec codec{configWith({filterStep("p"), filterStep("q")})};
    const auto results = codec.parse(R"({"row1": {"answer": "yes"}, "row2": {"answer": false}})", twoRows());
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0].passed);
    EXPECT_FALSE(results[1].passed);
}

TEST(SemanticFilterCodecTest, MixedStepsReadTheNestedEnvelope)
{
    const SemanticFilterCodec codec{
        configWith({mapStep("Classify", "SENTIMENT", {"POSITIVE", "NEGATIVE"}, "NEUTRAL"), filterStep("p"), filterStep("q")})};
    /// Keys in any case, as real models write them; row2 fails its second filter.
    const auto results = codec.parse(
        R"({"row1": {"sentiment": {"answer": "positive"}, "__filter_0": {"answer": true}, "__FILTER_1": {"answer": "YES"}},)"
        R"( "row2": {"SENTIMENT": {"answer": "awful"}, "__filter_0": {"answer": true}, "__filter_1": {"answer": false}}})",
        twoRows());
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0].passed);
    EXPECT_EQ(results[0].mapAnswers, (std::vector<std::string>{"POSITIVE"}));
    EXPECT_FALSE(results[1].passed);
    /// MAP answers are filled even for a dropped row, normalized as SEM_MAP would.
    EXPECT_EQ(results[1].mapAnswers, (std::vector<std::string>{"NEUTRAL"}));
}

TEST(SemanticFilterCodecTest, MixedStepsWithoutVerdictDropAndDefault)
{
    const SemanticFilterCodec codec{configWith({filterStep("p"), mapStep("Classify", "SENTIMENT", {}, "n/a")})};
    const auto results = codec.parse(R"({"row1": {"SENTIMENT": {"answer": "fine"}}})", twoRows());
    ASSERT_EQ(results.size(), 2);
    EXPECT_FALSE(results[0].passed);
    EXPECT_EQ(results[0].mapAnswers, (std::vector<std::string>{"fine"}));
    EXPECT_FALSE(results[1].passed);
    EXPECT_EQ(results[1].mapAnswers, (std::vector<std::string>{"n/a"}));
}

/// A nested answer that is not an object is taken as the verdict itself, like the reference's
/// `bool(col_data)`.
TEST(SemanticFilterCodecTest, MixedStepsAcceptBareVerdicts)
{
    const SemanticFilterCodec codec{configWith({mapStep("Classify", "SENTIMENT"), filterStep("p")})};
    const auto results
        = codec.parse(R"({"row1": {"SENTIMENT": {"answer": "x"}, "__filter_0": true}, "row2": {"__filter_0": "false"}})", twoRows());
    ASSERT_EQ(results.size(), 2);
    EXPECT_TRUE(results[0].passed);
    EXPECT_FALSE(results[1].passed);
}

}
