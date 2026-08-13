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
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Variant.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{

/// NOLINTBEGIN(cert-err58-cpp)

const ConfigField<uint64_t> THREADS{
    Identifier::parse("threads"),
    "Worker threads",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{4}};

const ConfigField<uint64_t> QUEUE_SIZE{
    Identifier::parse("queue_size"),
    "Queue size",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{1000}};

const ConfigField<bool> ENABLED{Identifier::parse("enabled"), "Feature flag", true};

/// Two fields with the same leaf name under different prefixes — triggers suffix collision.
const ConfigField<uint64_t> TIMEOUT_A{
    Identifier::parse("timeout"),
    "Timeout A",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{30}};

const ConfigField<uint64_t> TIMEOUT_B{
    Identifier::parse("timeout"),
    "Timeout B",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<int64_t, uint64_t>); },
    uint64_t{60}};

/// A field with no default — must be provided.
const ConfigField<std::string> REQUIRED_FIELD{Identifier::parse("address"), "Required address"};

/// NOLINTEND(cert-err58-cpp)

Schema<QualifiedErasedConfigField, Ordered> twoLevelSchema()
{
    return createConfigSchema(Identifier::parse("engine"), createConfigSchema(Identifier::parse("query"), THREADS, QUEUE_SIZE), ENABLED);
}

Schema<QualifiedErasedConfigField, Ordered> collidingSchema()
{
    return createConfigSchema(
        createConfigSchema(Identifier::parse("network"), TIMEOUT_A), createConfigSchema(Identifier::parse("cache"), TIMEOUT_B));
}

auto literal(const std::string& name, const ConfigLiteral& value)
{
    return LiteralConfigValue{QualifiedIdentifier::parse(name), value};
}

TEST(ConfigResolutionTest, suffixResolvesIntermediateQualification)
{
    const auto schema = twoLevelSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("query.threads", ConfigLiteral{int64_t{16}})};
    auto [resolved, errors] = resolveConfig(passed, schema);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 16);
}

TEST(ConfigResolutionTest, fullyQualifiedResolvesExactName)
{
    const auto schema = twoLevelSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("engine.query.threads", ConfigLiteral{int64_t{12}})};
    auto [resolved, errors] = resolveConfigFullyQualified(passed, schema);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 12);
}

TEST(ConfigResolutionTest, fullyQualifiedRejectsSuffixOnlyName)
{
    const auto schema = twoLevelSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("threads", ConfigLiteral{int64_t{8}})};
    auto [resolved, errors] = resolveConfigFullyQualified(passed, schema);
    EXPECT_FALSE(errors.empty());
    EXPECT_EQ(errors.unresolvableFields.size(), 1);
}

TEST(ConfigResolutionTest, suffixCollisionMakesLeafNameUnresolvable)
{
    const auto schema = collidingSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("timeout", ConfigLiteral{int64_t{99}})};
    auto [resolved, errors] = resolveConfig(passed, schema);
    EXPECT_FALSE(errors.empty());
    EXPECT_EQ(errors.unresolvableFields.size(), 1);
}

TEST(ConfigResolutionTest, suffixCollisionStillResolvableWithQualifier)
{
    const auto schema = collidingSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("network.timeout", ConfigLiteral{int64_t{99}})};
    auto [resolved, errors] = resolveConfig(passed, schema);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(TIMEOUT_A), 99);
    EXPECT_EQ(config.get(TIMEOUT_B), 60);
}

TEST(ConfigResolutionTest, allDefaultsPopulateWhenNothingPassed)
{
    const auto schema = twoLevelSchema();
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{}, schema);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 4);
    EXPECT_EQ(config.get(QUEUE_SIZE), 1000);
    EXPECT_EQ(config.get(ENABLED), true);
}

TEST(ConfigResolutionTest, missingRequiredFieldReportsError)
{
    const auto schema = createConfigSchema(Identifier::parse("server"), REQUIRED_FIELD, ENABLED);
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{}, schema);
    EXPECT_FALSE(errors.empty());
    EXPECT_EQ(errors.missingFields.size(), 1);
}

TEST(ConfigResolutionTest, frontendDefaultOverridesFieldDefault)
{
    const auto schema = twoLevelSchema();
    const Schema<ConfigFieldDefault, Ordered> frontendDefaults{
        ConfigFieldDefault{"engine.query.threads", [] { return ConfigLiteral{int64_t{32}}; }}};
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{}, schema, frontendDefaults);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 32);
    EXPECT_EQ(config.get(QUEUE_SIZE), 1000);
}

TEST(ConfigResolutionTest, passedValueTakesPrecedenceOverFrontendDefault)
{
    const auto schema = twoLevelSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("engine.query.threads", ConfigLiteral{int64_t{2}})};
    const Schema<ConfigFieldDefault, Ordered> frontendDefaults{
        ConfigFieldDefault{"engine.query.threads", [] { return ConfigLiteral{int64_t{32}}; }}};
    auto [resolved, errors] = resolveConfigFullyQualified(passed, schema, frontendDefaults);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 2);
}

TEST(ConfigResolutionTest, factoryRejectsWrongType)
{
    const auto schema = twoLevelSchema();
    const Schema<LiteralConfigValue, Ordered> passed{literal("engine.query.threads", ConfigLiteral{std::string{"not_a_number"}})};
    auto [resolved, errors] = resolveConfigFullyQualified(passed, schema);
    EXPECT_FALSE(errors.empty());
    EXPECT_EQ(errors.failedInstantiations.size(), 1);
}

TEST(ConfigMergeTest, laterLayerOverridesEarlier)
{
    auto yaml = Schema<LiteralConfigValue, Ordered>{literal("worker.threads", ConfigLiteral{int64_t{2}})};
    auto cli = Schema<LiteralConfigValue, Ordered>{literal("worker.threads", ConfigLiteral{int64_t{8}})};
    auto result = mergeConfigLayers(
        {ConfigLayer{.name = "yaml", .literals = std::move(yaml)}, ConfigLayer{.name = "cli", .literals = std::move(cli)}});
    ASSERT_EQ(result.overwrites.size(), 1);
    EXPECT_EQ(result.overwrites[0].appliedLayer, "cli");
    EXPECT_EQ(std::get<int64_t>(result.overwrites[0].appliedValue), 8);
    auto merged = result.literals.getFieldByName(QualifiedIdentifier::parse("worker.threads"));
    ASSERT_TRUE(merged.has_value());
    EXPECT_EQ(std::get<int64_t>(merged->getValue()), 8);
}

TEST(ConfigMergeTest, disjointLayersMerge)
{
    auto layer1 = Schema<LiteralConfigValue, Ordered>{literal("a.x", ConfigLiteral{int64_t{1}})};
    auto layer2 = Schema<LiteralConfigValue, Ordered>{literal("b.y", ConfigLiteral{int64_t{2}})};
    auto result = mergeConfigLayers(
        {ConfigLayer{.name = "l1", .literals = std::move(layer1)}, ConfigLayer{.name = "l2", .literals = std::move(layer2)}});
    EXPECT_TRUE(result.overwrites.empty());
    EXPECT_EQ(result.literals.size(), 2);
}

TEST(ConfigParsingTest, parseCommandLineConfigRoundTrips)
{
    auto parsed = parseCommandLineConfig({"--engine.query.threads=8", "--engine.enabled=true"});
    const auto schema = twoLevelSchema();
    auto [resolved, errors] = resolveConfigFullyQualified(parsed, schema);
    EXPECT_TRUE(errors.empty()) << errors;
    const InstantiatedConfig config{std::move(resolved)};
    EXPECT_EQ(config.get(THREADS), 8);
    EXPECT_EQ(config.get(ENABLED), true);
}

TEST(ConfigParsingTest, parseConfigLiteralInfersTypes)
{
    EXPECT_TRUE(std::holds_alternative<int64_t>(parseConfigLiteral("42")));
    EXPECT_TRUE(std::holds_alternative<double>(parseConfigLiteral("3.14")));
    EXPECT_TRUE(std::holds_alternative<bool>(parseConfigLiteral("true")));
    EXPECT_TRUE(std::holds_alternative<bool>(parseConfigLiteral("FALSE")));
    EXPECT_TRUE(std::holds_alternative<std::string>(parseConfigLiteral("hello")));
    EXPECT_TRUE(std::holds_alternative<int64_t>(parseConfigLiteral("-5")));
}

}
}
