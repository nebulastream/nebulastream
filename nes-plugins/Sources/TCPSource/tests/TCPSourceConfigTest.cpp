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
#include <optional>
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
#include <TCPSource.hpp>

namespace NES
{
namespace
{
std::expected<TCPSourceConfig, Exception> resolveTcpConfig(
    ConfigLiteral port,
    std::optional<ConfigLiteral> flushInterval = std::nullopt,
    std::optional<ConfigLiteral> connectTimeout = std::nullopt)
{
    std::vector<LiteralConfigValue> values{
        {QualifiedIdentifier::parse("TCP_SOURCE.SOCKET_HOST"), std::string{"localhost"}},
        {QualifiedIdentifier::parse("TCP_SOURCE.SOCKET_PORT"), std::move(port)}};
    if (flushInterval)
    {
        values.emplace_back(QualifiedIdentifier::parse("TCP_SOURCE.FLUSH_INTERVAL_MS"), std::move(*flushInterval));
    }
    if (connectTimeout)
    {
        values.emplace_back(QualifiedIdentifier::parse("TCP_SOURCE.CONNECT_TIMEOUT_MS"), std::move(*connectTimeout));
    }
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{values}, TCPSource::getConfigSchema());
    if (!errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", errors)};
    }
    return TCPSourceConfig::fromConfig(InstantiatedConfig{resolved});
}
}

TEST(TCPSourceConfigTest, KeepsAsioDefaultsInMilliseconds)
{
    const auto config = resolveTcpConfig(int64_t{4242});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->flushIntervalMs, 100U);
    EXPECT_EQ(config->connectTimeoutMs, 5000U);
}

TEST(TCPSourceConfigTest, AcceptsExplicitMillisecondIntervals)
{
    const auto config = resolveTcpConfig(int64_t{4242}, int64_t{0}, int64_t{60000});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->flushIntervalMs, 0U);
    EXPECT_EQ(config->connectTimeoutMs, 60000U);
}

TEST(TCPSourceConfigTest, AcceptsBoundaryPorts)
{
    EXPECT_TRUE(resolveTcpConfig(int64_t{1}).has_value());
    EXPECT_TRUE(resolveTcpConfig(int64_t{65535}).has_value());
}

TEST(TCPSourceConfigTest, RejectsInvalidPorts)
{
    for (const int64_t port : {-1, 0, 65536})
    {
        EXPECT_FALSE(resolveTcpConfig(port).has_value()) << port;
    }
}

TEST(TCPSourceConfigTest, RejectsIntervalsOutsideUnsigned32BitRange)
{
    for (const int64_t interval : {int64_t{-1}, int64_t{4294967296}})
    {
        EXPECT_FALSE(resolveTcpConfig(int64_t{4242}, interval).has_value()) << interval;
        EXPECT_FALSE(resolveTcpConfig(int64_t{4242}, std::nullopt, interval).has_value()) << interval;
    }
}

TEST(TCPSourceConfigTest, RequiresNumericLiterals)
{
    EXPECT_FALSE(resolveTcpConfig(std::string{"4242"}).has_value());
    EXPECT_FALSE(resolveTcpConfig(int64_t{4242}, std::string{"100"}).has_value());
    EXPECT_FALSE(resolveTcpConfig(int64_t{4242}, std::nullopt, std::string{"5000"}).has_value());
}
}
