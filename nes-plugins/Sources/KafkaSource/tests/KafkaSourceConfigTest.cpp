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
#include <librdkafka/rdkafkacpp.h>
#include <ErrorHandling.hpp>
#include <KafkaSource.hpp>

/// Invalid configs are inline values here so that no rejected config is next to a source example where a user could copy it.

namespace NES
{
namespace
{
std::expected<KafkaSourceConfig, Exception> resolveKafkaConfigValues(const std::vector<LiteralConfigValue>& values)
{
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{values}, KafkaSource::getConfigSchema());
    if (!errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", errors)};
    }
    return KafkaSourceConfig::fromConfig(InstantiatedConfig{resolved});
}

LiteralConfigValue option(const std::string& name, ConfigLiteral value)
{
    return {QualifiedIdentifier::parse("KAFKA_SOURCE." + name), std::move(value)};
}

/// A config with the three required keys, plus the given optional ones.
std::expected<KafkaSourceConfig, Exception> resolveKafkaConfig(std::vector<LiteralConfigValue> optional)
{
    std::vector<LiteralConfigValue> values{
        option("BROKERS", std::string{"localhost:9092"}), option("TOPIC", std::string{"sensors"}), option("GROUP_ID", std::string{"nes"})};
    for (auto& value : optional)
    {
        values.push_back(std::move(value));
    }
    return resolveKafkaConfigValues(values);
}
}

TEST(KafkaSourceConfigTest, RejectsMissingBrokers)
{
    EXPECT_FALSE(resolveKafkaConfigValues({option("TOPIC", std::string{"sensors"}), option("GROUP_ID", std::string{"nes"})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsMissingTopic)
{
    EXPECT_FALSE(
        resolveKafkaConfigValues({option("BROKERS", std::string{"localhost:9092"}), option("GROUP_ID", std::string{"nes"})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsMissingGroupId)
{
    EXPECT_FALSE(
        resolveKafkaConfigValues({option("BROKERS", std::string{"localhost:9092"}), option("TOPIC", std::string{"sensors"})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsEmptyRequiredValues)
{
    for (const std::string empty : {"BROKERS", "TOPIC", "GROUP_ID"})
    {
        std::vector<LiteralConfigValue> values;
        for (const std::string key : {"BROKERS", "TOPIC", "GROUP_ID"})
        {
            values.push_back(option(key, key == empty ? std::string{} : std::string{"value"}));
        }
        EXPECT_FALSE(resolveKafkaConfigValues(values).has_value()) << empty;
    }
}

TEST(KafkaSourceConfigTest, RejectsTopicListsAndPatterns)
{
    for (const auto* topic : {"a,b", "^sensors.*", "has space", ".."})
    {
        EXPECT_FALSE(resolveKafkaConfig({option("TOPIC", std::string{topic})}).has_value()) << topic;
    }
}

TEST(KafkaSourceConfigTest, RejectsInvalidAutoOffsetReset)
{
    EXPECT_FALSE(resolveKafkaConfig({option("AUTO_OFFSET_RESET", std::string{"none"})}).has_value());
    EXPECT_FALSE(resolveKafkaConfig({option("AUTO_OFFSET_RESET", std::string{"beginning"})}).has_value());
    EXPECT_FALSE(resolveKafkaConfig({option("AUTO_OFFSET_RESET", int64_t{1})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsInvalidCommitMode)
{
    EXPECT_FALSE(resolveKafkaConfig({option("COMMIT_MODE", std::string{"MANUAL"})}).has_value());
    EXPECT_FALSE(resolveKafkaConfig({option("COMMIT_MODE", int64_t{1})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsNegativeFlushInterval)
{
    EXPECT_FALSE(resolveKafkaConfig({option("FLUSH_INTERVAL_MS", int64_t{-1})}).has_value());
}

TEST(KafkaSourceConfigTest, RequiresNumericFlushInterval)
{
    EXPECT_FALSE(resolveKafkaConfig({option("FLUSH_INTERVAL_MS", std::string{"100"})}).has_value());
}

TEST(KafkaSourceConfigTest, RejectsUnknownKey)
{
    EXPECT_FALSE(resolveKafkaConfig({option("PARTITIONS", std::string{"0"})}).has_value());
}

TEST(KafkaSourceConfigTest, KeepsDefaults)
{
    const auto config = resolveKafkaConfig({});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->brokers, "localhost:9092");
    EXPECT_EQ(config->topic, "sensors");
    EXPECT_EQ(config->groupId, "nes");
    EXPECT_EQ(config->autoOffsetReset, "latest");
    EXPECT_TRUE(config->synchronousCommit);
    EXPECT_EQ(config->flushIntervalMs, 0);
    EXPECT_EQ(config->implicitMessageDelimiter, "\n");
}

TEST(KafkaSourceConfigTest, KeepsEveryOption)
{
    const auto config = resolveKafkaConfig(
        {option("AUTO_OFFSET_RESET", std::string{"earliest"}),
         option("COMMIT_MODE", std::string{"ASYNC"}),
         option("FLUSH_INTERVAL_MS", int64_t{250}),
         option("IMPLICIT_MESSAGE_DELIMITER", std::string{";"})});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->autoOffsetReset, "earliest");
    EXPECT_FALSE(config->synchronousCommit);
    EXPECT_EQ(config->flushIntervalMs, 250);
    EXPECT_EQ(config->implicitMessageDelimiter, ";");
}

TEST(KafkaSourceConfigTest, EnumValuesAreCaseInsensitive)
{
    const auto config
        = resolveKafkaConfig({option("AUTO_OFFSET_RESET", std::string{"EARLIEST"}), option("COMMIT_MODE", std::string{"async"})});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->autoOffsetReset, "earliest");
    EXPECT_FALSE(config->synchronousCommit);
}

TEST(KafkaSourceConfigTest, KeepsEmptyMessageDelimiter)
{
    const auto config = resolveKafkaConfig({option("IMPLICIT_MESSAGE_DELIMITER", std::string{})});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->implicitMessageDelimiter, "");
}

TEST(KafkaSourceConfigTest, ConsumerConfCarriesTheOptions)
{
    const auto config = resolveKafkaConfig({option("AUTO_OFFSET_RESET", std::string{"earliest"})});
    ASSERT_TRUE(config.has_value()) << config.error();
    const KafkaSource source{*config};
    const auto conf = source.buildConsumerConf();
    const auto value = [&](const std::string& key)
    {
        std::string result;
        EXPECT_EQ(conf->get(key, result), RdKafka::Conf::CONF_OK) << key;
        return result;
    };
    EXPECT_EQ(value("bootstrap.servers"), "localhost:9092");
    EXPECT_EQ(value("group.id"), "nes");
    EXPECT_EQ(value("auto.offset.reset"), "smallest");
    EXPECT_EQ(value("enable.auto.commit"), "false");
}
}
