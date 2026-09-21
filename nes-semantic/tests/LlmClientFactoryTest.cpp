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

#include <LlmClientFactory.hpp>

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <BaseUnitTest.hpp>
#include <CurlLlmClient.hpp>
#include <ErrorHandling.hpp>
#include <MockLlmClient.hpp>
#include <SemanticModelConfig.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

namespace NES
{

namespace
{

/// Free-text model: no OUTPUT_VALUES, so the cascade is bypassed entirely (plan §2.3).
SemanticModelConfig freeTextConfig(const std::string& baseUrl)
{
    return SemanticModelConfig{
        .baseUrl = baseUrl,
        .model = "test-model",
        .apiKeyEnv = std::nullopt,
        .steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP, .prompt = "Classify the sentiment", .outputValues = {}, .defaultValue = ""}}};
}

SemanticModelConfig restrictedConfig(const std::string& baseUrl)
{
    return SemanticModelConfig{
        .baseUrl = baseUrl,
        .model = "test-model",
        .apiKeyEnv = std::nullopt,
        .steps = {SemanticStep{
            .kind = SemanticStep::Kind::MAP,
            .prompt = "Classify the sentiment",
            .outputValues = {"POSITIVE", "NEGATIVE"},
            .defaultValue = ""}}};
}

}

class LlmClientFactoryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("LlmClientFactoryTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(LlmClientFactoryTest, SelectsMockClientForMockScheme)
{
    auto client = createLlmClient(restrictedConfig("mock://echo"), {"sentiment"});
    EXPECT_NE(dynamic_cast<MockLlmClient*>(client.get()), nullptr);
}

TEST_F(LlmClientFactoryTest, SelectsCurlClientForHttpSchemes)
{
    auto client = createLlmClient(restrictedConfig("http://localhost:11434/v1"), {"sentiment"});
    EXPECT_NE(dynamic_cast<CurlLlmClient*>(client.get()), nullptr);

    auto secureClient = createLlmClient(restrictedConfig("https://example.invalid/v1"), {"sentiment"});
    EXPECT_NE(dynamic_cast<CurlLlmClient*>(secureClient.get()), nullptr);
}

TEST_F(LlmClientFactoryTest, RejectsUnknownScheme)
{
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(createLlmClient(restrictedConfig("ftp://localhost:11434"), {"sentiment"})), NES::ErrorCode::CannotLoadModel);
}

TEST_F(LlmClientFactoryTest, RejectsUnknownMockBehaviour)
{
    ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(createLlmClient(restrictedConfig("mock://bogus"), {"sentiment"})), NES::ErrorCode::CannotLoadModel);
}

TEST_F(LlmClientFactoryTest, MockEchoUppercasesAndKeysByOutputFieldName)
{
    auto client = createLlmClient(freeTextConfig("mock://echo"), {"sentiment"});
    const auto result = client->map("a great product");

    ASSERT_TRUE(result.contains("sentiment"));
    EXPECT_EQ(result.at("sentiment").answer, "A GREAT PRODUCT");
    EXPECT_DOUBLE_EQ(result.at("sentiment").confidence, 1.0);
}

/// The fast twin of systest query 2 (nes-systests/semantic/SemMap.test): one input per rung of
/// the normalisation cascade, computed against `levenshteinRatio` (cutoff 0.6) and the
/// declaration-order containment loop in normalizeAnswer (plan §2.3).
TEST_F(LlmClientFactoryTest, MockEchoRunsTheNormalisationCascade)
{
    auto client = createLlmClient(restrictedConfig("mock://echo"), {"sentiment"});

    const std::vector<std::pair<std::string, std::string>> cascade{
        {"POSITIVE", "POSITIVE"},          /// exact match
        {"negative", "NEGATIVE"},          /// uppercase exact match
        {"POSITIVE(unsure)", "POSITIVE"},  /// strip parenthetical
        {"clearly-negative", "NEGATIVE"},  /// containment
        {"pozitive", "POSITIVE"},          /// fuzzy, ratio 0.9375
        {"zzzz", ""},                      /// below cutoff (0.333) -> default-fill
    };
    for (const auto& [input, expected] : cascade)
    {
        const auto result = client->map(input);
        EXPECT_EQ(result.at("sentiment").answer, expected) << "input: " << input;
        /// The mock always reports confidence 1.0 on the happy path — even a default-filled row
        /// (mirroring a live LLM answering below the cascade's cutoff with confidence 0.9).
        EXPECT_DOUBLE_EQ(result.at("sentiment").confidence, 1.0) << "input: " << input;
    }
}

TEST_F(LlmClientFactoryTest, MockUnparseableDefaultFills)
{
    auto client = createLlmClient(restrictedConfig("mock://unparseable"), {"sentiment"});
    const auto result = client->map("anything at all");

    EXPECT_EQ(result.at("sentiment").answer, "");
    EXPECT_DOUBLE_EQ(result.at("sentiment").confidence, 0.0);
}

TEST_F(LlmClientFactoryTest, MockUnreachableThrowsInferenceRuntimeFailure)
{
    auto client = createLlmClient(restrictedConfig("mock://unreachable"), {"sentiment"});
    ASSERT_EXCEPTION_ERRORCODE(client->map("anything"), NES::ErrorCode::InferenceRuntimeFailure);
}

}