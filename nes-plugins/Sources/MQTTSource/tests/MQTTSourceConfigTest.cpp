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

#include <cstdint>
#include <expected>
#include <string>
#include <utility>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <MQTTSource.hpp>

/// Valid configs are the connector's conn-test/configs/*.nesql files, which conntest-MQTTSource-configs binds as committed.
/// Invalid configs are inline values here, not config files,
/// so that no rejected config is next to the connector's examples where a user could copy it.

namespace NES
{
namespace
{
std::expected<MQTTSourceConfig, Exception> resolveMqttConfigValues(const std::vector<LiteralConfigValue>& values)
{
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{values}, MQTTSource::getConfigSchema());
    if (!errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", errors)};
    }
    return MQTTSourceConfig::fromConfig(InstantiatedConfig{resolved});
}

/// A config with the two required keys, plus the given optional ones.
std::expected<MQTTSourceConfig, Exception> resolveMqttConfig(std::vector<LiteralConfigValue> optional)
{
    std::vector<LiteralConfigValue> values{
        {QualifiedIdentifier::parse("MQTT_SOURCE.SERVER_URI"), std::string{"tcp://localhost:1883"}},
        {QualifiedIdentifier::parse("MQTT_SOURCE.TOPIC"), std::string{"sensors"}}};
    for (auto& value : optional)
    {
        values.push_back(std::move(value));
    }
    return resolveMqttConfigValues(values);
}

LiteralConfigValue qos(ConfigLiteral value)
{
    return {QualifiedIdentifier::parse("MQTT_SOURCE.QOS"), std::move(value)};
}

LiteralConfigValue flushInterval(ConfigLiteral value)
{
    return {QualifiedIdentifier::parse("MQTT_SOURCE.FLUSH_INTERVAL_MS"), std::move(value)};
}
}

TEST(MQTTSourceConfigTest, RejectsQosOutsideZeroToTwo)
{
    for (const int64_t level : {-1, 3})
    {
        EXPECT_FALSE(resolveMqttConfig({qos(level)}).has_value()) << level;
    }
}

TEST(MQTTSourceConfigTest, RejectsNegativeFlushInterval)
{
    EXPECT_FALSE(resolveMqttConfig({flushInterval(int64_t{-1})}).has_value());
}

TEST(MQTTSourceConfigTest, RequiresNumericLiterals)
{
    EXPECT_FALSE(resolveMqttConfig({qos(std::string{"1"})}).has_value());
    EXPECT_FALSE(resolveMqttConfig({flushInterval(std::string{"100"})}).has_value());
}

TEST(MQTTSourceConfigTest, RejectsMissingServerUri)
{
    EXPECT_FALSE(resolveMqttConfigValues({{QualifiedIdentifier::parse("MQTT_SOURCE.TOPIC"), std::string{"sensors"}}}).has_value());
}

TEST(MQTTSourceConfigTest, RejectsMissingTopic)
{
    EXPECT_FALSE(
        resolveMqttConfigValues({{QualifiedIdentifier::parse("MQTT_SOURCE.SERVER_URI"), std::string{"tcp://localhost:1883"}}}).has_value());
}

TEST(MQTTSourceConfigTest, RejectsUnknownKey)
{
    EXPECT_FALSE(resolveMqttConfig({{QualifiedIdentifier::parse("MQTT_SOURCE.RETAIN"), std::string{"true"}}}).has_value());
}

/// The checks below are not about which configs are valid but about the values that a valid config resolves to,
/// which binding a committed config does not show.

TEST(MQTTSourceConfigTest, KeepsDefaults)
{
    const auto config = resolveMqttConfig({});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->qos, 1);
    EXPECT_EQ(config->flushIntervalMs, 0);
    EXPECT_EQ(config->implicitMessageDelimiter, "\n");
    EXPECT_FALSE(config->logMessages);
    /// The source replaces the token with a fresh client id, so that two sources never share a session.
    EXPECT_EQ(config->clientId, GENERATE_CLIENT_ID_TOKEN);
}

TEST(MQTTSourceConfigTest, KeepsEveryQosLevel)
{
    for (const int64_t level : {0, 1, 2})
    {
        const auto config = resolveMqttConfig({qos(level)});
        ASSERT_TRUE(config.has_value()) << level << ": " << config.error();
        EXPECT_EQ(config->qos, level);
    }
}

/// An empty delimiter lets a row span messages, which a committed config cannot show,
/// because the loader publishes one row per message.
TEST(MQTTSourceConfigTest, KeepsEmptyMessageDelimiter)
{
    const auto config = resolveMqttConfig({{QualifiedIdentifier::parse("MQTT_SOURCE.IMPLICIT_MESSAGE_DELIMITER"), std::string{}}});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->implicitMessageDelimiter, "");
}
}
