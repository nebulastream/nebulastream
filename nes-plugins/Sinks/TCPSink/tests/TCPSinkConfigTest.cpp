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
#include <TCPSink.hpp>

namespace NES
{
namespace
{
std::expected<TCPSinkConfig, Exception> resolveTcpSinkConfig(ConfigLiteral port, std::optional<LiteralConfigValue> extra = std::nullopt)
{
    std::vector<LiteralConfigValue> values{
        {QualifiedIdentifier::parse("TCP_SINK.SOCKET_HOST"), std::string{"localhost"}},
        {QualifiedIdentifier::parse("TCP_SINK.SOCKET_PORT"), std::move(port)}};
    if (extra)
    {
        values.push_back(std::move(*extra));
    }
    auto [resolved, errors] = resolveConfig(Schema<LiteralConfigValue, Ordered>{values}, TCPSink::getConfigSchema());
    if (!errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("{}", errors)};
    }
    return TCPSinkConfig::fromConfig(InstantiatedConfig{resolved});
}
}

TEST(TCPSinkConfigTest, KeepsChannelDefaults)
{
    const auto config = resolveTcpSinkConfig(int64_t{4242});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->socketHost, "localhost");
    EXPECT_EQ(config->socketPort, 4242U);
    EXPECT_EQ(config->connectTimeoutMs, 10000U);
    EXPECT_EQ(config->closeTimeoutMs, 10000U);
    EXPECT_EQ(config->maxQueuedBuffers, 16U);
}

TEST(TCPSinkConfigTest, AcceptsExplicitQueueLimit)
{
    const auto config
        = resolveTcpSinkConfig(int64_t{4242}, LiteralConfigValue{QualifiedIdentifier::parse("TCP_SINK.MAX_QUEUED_BUFFERS"), int64_t{4}});
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->maxQueuedBuffers, 4U);
}

TEST(TCPSinkConfigTest, AcceptsBoundaryPorts)
{
    EXPECT_TRUE(resolveTcpSinkConfig(int64_t{1}).has_value());
    EXPECT_TRUE(resolveTcpSinkConfig(int64_t{65535}).has_value());
}

TEST(TCPSinkConfigTest, RejectsInvalidPorts)
{
    for (const int64_t port : {-1, 0, 65536})
    {
        EXPECT_FALSE(resolveTcpSinkConfig(port).has_value()) << port;
    }
}

TEST(TCPSinkConfigTest, RejectsInvalidTimeouts)
{
    for (const auto* field : {"TCP_SINK.CONNECT_TIMEOUT_MS", "TCP_SINK.CLOSE_TIMEOUT_MS"})
    {
        for (const int64_t timeout : {int64_t{-1}, int64_t{0}, int64_t{4294967296}})
        {
            EXPECT_FALSE(resolveTcpSinkConfig(int64_t{4242}, LiteralConfigValue{QualifiedIdentifier::parse(field), timeout}).has_value())
                << field << ": " << timeout;
        }
    }
}

TEST(TCPSinkConfigTest, RejectsNonpositiveQueueLimits)
{
    for (const int64_t count : {-1, 0})
    {
        EXPECT_FALSE(
            resolveTcpSinkConfig(int64_t{4242}, LiteralConfigValue{QualifiedIdentifier::parse("TCP_SINK.MAX_QUEUED_BUFFERS"), count})
                .has_value());
    }
}

TEST(TCPSinkConfigTest, RequiresNumericLiterals)
{
    EXPECT_FALSE(resolveTcpSinkConfig(std::string{"4242"}).has_value());
    EXPECT_FALSE(resolveTcpSinkConfig(
                     int64_t{4242}, LiteralConfigValue{QualifiedIdentifier::parse("TCP_SINK.CONNECT_TIMEOUT_MS"), std::string{"10000"}})
                     .has_value());
}
}
