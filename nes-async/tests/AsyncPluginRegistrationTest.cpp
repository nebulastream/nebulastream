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

#include <string>
#include <tuple>
#include <unordered_map>

#include <Async/AsyncWiring.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Proves the build wiring: the two halves of the asynchronous operator are reachable
/// through the sink and source registries, and their descriptors validate. Without this,
/// a query would only fail much later, when the plan is instantiated on the worker.
class AsyncPluginRegistrationTest : public ::testing::Test
{
};

TEST_F(AsyncPluginRegistrationTest, HandoffSinkIsRegistered)
{
    EXPECT_TRUE(SinkRegistry::instance().find("Handoff").has_value());
    /// Registry keys are matched case-insensitively.
    EXPECT_TRUE(SinkRegistry::instance().find("handoff").has_value());
    EXPECT_TRUE(SinkValidationRegistry::instance().find("Handoff").has_value());
}

TEST_F(AsyncPluginRegistrationTest, AsyncSourceIsRegistered)
{
    EXPECT_TRUE(SourceRegistry::instance().find("Async").has_value());
    EXPECT_TRUE(SourceRegistry::instance().find("async").has_value());
    EXPECT_TRUE(SourceValidationRegistry::instance().find("Async").has_value());
}

TEST_F(AsyncPluginRegistrationTest, HandoffSinkValidatesItsConfiguration)
{
    const auto validator = SinkValidationRegistry::instance().find("Handoff");
    ASSERT_TRUE(validator.has_value());

    const auto validated = (*validator)(SinkValidationRegistryArguments{{{"CHANNEL", "abc"}, {"CHANNEL_CAPACITY", "8"}}});
    /// Buffers cross the channel unformatted, so the sink pins its output format. Anything else
    /// would make the pipelining phase insert a formatting emit and write text into the channel.
    ASSERT_TRUE(validated.contains("OUTPUT_FORMAT"));
    EXPECT_EQ(std::get<std::string>(validated.at("OUTPUT_FORMAT")), "NATIVE");

    /// Even an explicit CSV is overridden rather than honoured.
    const auto overridden
        = (*validator)(SinkValidationRegistryArguments{{{"CHANNEL", "abc"}, {"OUTPUT_FORMAT", "CSV"}}});
    EXPECT_EQ(std::get<std::string>(overridden.at("OUTPUT_FORMAT")), "NATIVE");

    /// CHANNEL has no default, so leaving it out must be rejected rather than silently
    /// producing a sink that never finds its counterpart.
    EXPECT_THROW(std::ignore = (*validator)(SinkValidationRegistryArguments{{{"CHANNEL_CAPACITY", "8"}}}), Exception);
    /// An unknown key is a typo, not something to ignore.
    EXPECT_THROW(std::ignore = (*validator)(SinkValidationRegistryArguments{{{"CHANNEL", "abc"}, {"NONSENSE", "1"}}}), Exception);
}

TEST_F(AsyncPluginRegistrationTest, AsyncSourceValidatesItsConfiguration)
{
    const auto validator = SourceValidationRegistry::instance().find("Async");
    ASSERT_TRUE(validator.has_value());

    const Schema<UnqualifiedUnboundField, Ordered> inputSchema{
        UnqualifiedUnboundField{Identifier::parse("reviewText"), DataType::Type::VARSIZED}};

    EXPECT_NO_THROW(
        std::ignore = (*validator)(
            SourceValidationRegistryArguments{
                {{"CHANNEL", "abc"},
                 {"EXECUTOR_TYPE", "Delay"},
                 {"EXECUTOR_CONFIG", encodeConfig({{"delay_ms", "10"}})},
                 {"INPUT_SCHEMA", encodeSchema(inputSchema)},
                 {"BATCH_SIZE", "10"},
                 {"MAX_CONCURRENCY", "4"},
                 {"PRESERVE_ORDER", "true"}}}));

    /// EXECUTOR_TYPE and INPUT_SCHEMA are both mandatory.
    EXPECT_THROW(std::ignore = (*validator)(SourceValidationRegistryArguments{{{"CHANNEL", "abc"}}}), Exception);
}

TEST_F(AsyncPluginRegistrationTest, WiringSurvivesARoundTrip)
{
    /// The split rule writes these two values into the source descriptor, and the source reads
    /// them back on the worker. Prompts contain arbitrary characters, so the encoding has to
    /// survive quotes, separators and newlines.
    const std::unordered_map<std::string, std::string> config{
        {"prompt", R"(Determine if the review is "positive"; or negative, and say why)"},
        {"delay_ms", "100"},
        {"multiline", "first line\nsecond line"}};
    EXPECT_EQ(decodeConfig(encodeConfig(config)), config);

    const Schema<UnqualifiedUnboundField, Ordered> schema{
        UnqualifiedUnboundField{Identifier::parse("reviewId"), DataType::Type::UINT64},
        UnqualifiedUnboundField{Identifier::parse("reviewText"), DataType::Type::VARSIZED}};
    const auto decoded = decodeSchema(encodeSchema(schema));

    ASSERT_EQ(decoded.size(), schema.size());
    EXPECT_EQ(decoded, schema);
}

}
