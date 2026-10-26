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

#include <MockSemanticBackend.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SemanticBackend.hpp>
#include <SemanticMapCodec.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

SemanticModelConfig configFor(std::string backend, std::string endpoint)
{
    SemanticModelConfig config;
    config.backend = std::move(backend);
    config.endpoint = std::move(endpoint);
    config.modelName = "m";
    config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP, .prompt = "Classify", .outputColumn = "SENTIMENT", .outputValues = {}, .defaultValue = "DEFAULT"}};
    return config;
}

/// The mock as the backend factory of the synchronous path built it: every step's column gets the answer.
std::unique_ptr<SemanticBackend> mockFor(const SemanticModelConfig& config)
{
    std::vector<std::string> columns;
    for (const auto& step : config.steps)
    {
        columns.push_back(step.outputColumn);
    }
    return std::make_unique<MockSemanticBackend>(config.endpoint, std::move(columns));
}

/// Runs one row through codec -> mock backend -> codec, the way the physical operator does.
std::string roundTrip(const SemanticModelConfig& config, const std::string& text)
{
    const SemanticMapCodec codec{config};
    const std::vector<RowPayload> rows{RowPayload{.rowId = "row1", .fields = {{"REVIEWTEXT", text}}}};
    auto backend = mockFor(config);
    const auto response = backend->complete(
        CompletionRequest{
            .prompt = codec.buildPrompt(rows), .modelName = config.modelName, .timeout = {}, .connectTimeout = {}, .maxRetries = 0});
    EXPECT_TRUE(response.has_value());
    return codec.parse(response.value_or(""), rows).front().front();
}

}

TEST(MockSemanticBackendTest, RejectsAnUnknownBehaviour)
{
    ASSERT_EXCEPTION_ERRORCODE((void)mockFor(configFor("mock", "echoo")), ErrorCode::InvalidSemanticModel);
}

/// SEM_CALL waits for the delay on a timer and asks `respond` for the answer itself.
TEST(MockSemanticBackendTest, RespondDoesNotWait)
{
    const MockSemanticBackend mock{"label:X@5000", {"SENTIMENT"}};
    EXPECT_EQ(mock.getDelay(), std::chrono::milliseconds{5000});
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(mock.respond("...\nData: {\"row1\": \"a\"}").has_value());
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds{1000});
}

TEST(MockSemanticBackendTest, MockEchoesThroughTheRealCodec)
{
    EXPECT_EQ(roundTrip(configFor("mock", "echo"), "café \"quoted\"\nline"), "CAFé \"QUOTED\"\nLINE");
}

TEST(MockSemanticBackendTest, MockEchoJoinsJsonObjectPayloads)
{
    auto config = configFor("mock", "echo");
    config.payloadFormat = PayloadFormat::JSON_OBJECT;
    EXPECT_EQ(roundTrip(config, "fine"), "FINE");
}

TEST(MockSemanticBackendTest, MockLabelAnswersEveryRowWithTheLabel)
{
    auto config = configFor("mock", "label:positive");
    EXPECT_EQ(roundTrip(config, "anything"), "positive");
    config.steps.front().outputValues = {"POSITIVE", "NEGATIVE"};
    EXPECT_EQ(roundTrip(config, "anything"), "POSITIVE");
}

/// The mock answers with lower-cased field names, which the codec must still match.
TEST(MockSemanticBackendTest, MockUsesLowerCaseFieldNames)
{
    auto backend = mockFor(configFor("mock", "label:X"));
    const auto response = backend->complete(
        CompletionRequest{
            .prompt = "...\nData: {\"row1\": \"a\"}", .modelName = "m", .timeout = {}, .connectTimeout = {}, .maxRetries = 0});
    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(nlohmann::json::parse(*response), nlohmann::json::parse(R"({"row1": {"sentiment": {"answer": "X", "confidence": 1.0}}})"));
}

TEST(MockSemanticBackendTest, MockUnparseableDefaultFills)
{
    EXPECT_EQ(roundTrip(configFor("mock", "unparseable"), "anything"), "DEFAULT");
}

TEST(MockSemanticBackendTest, MockFailFailsLikeAnUnreachableEndpoint)
{
    auto backend = mockFor(configFor("mock", "fail"));
    const auto response
        = backend->complete(CompletionRequest{.prompt = "p", .modelName = "m", .timeout = {}, .connectTimeout = {}, .maxRetries = 5});
    ASSERT_FALSE(response.has_value());
    EXPECT_EQ(response.error().kind, BackendError::Kind::UNREACHABLE);
}

/// The delay suffix is what lets a hermetic test stand in for a model's latency.
TEST(MockSemanticBackendTest, MockDelaySuffixMakesARequestTakeTime)
{
    EXPECT_TRUE(MockSemanticBackend::isValidBehaviour("echo@50"));
    EXPECT_EQ(roundTrip(configFor("mock", "echo@50"), "still echoes"), "STILL ECHOES");

    const auto start = std::chrono::steady_clock::now();
    (void)roundTrip(configFor("mock", "echo@50"), "waits");
    EXPECT_GE(std::chrono::steady_clock::now() - start, std::chrono::milliseconds{50});
}

TEST(MockSemanticBackendTest, MockDelaySuffixOnlyCountsWhenItIsAllDigits)
{
    /// A label is free text, so an '@' in it must not be read as a delay.
    EXPECT_TRUE(MockSemanticBackend::isValidBehaviour("label:a@b"));
    EXPECT_EQ(roundTrip(configFor("mock", "label:a@b"), "anything"), "a@b");

    EXPECT_FALSE(MockSemanticBackend::isValidBehaviour("echo@"));
    EXPECT_FALSE(MockSemanticBackend::isValidBehaviour("nonsense@10"));
}

}
