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
#include <unordered_map>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <TCPSource.hpp>

/// Invalid configs are inline maps, not config files,
/// so that no rejected config is next to the connector's examples where a user could copy it.

namespace NES
{
namespace
{
class TCPSourceConfigTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("TCPSourceConfigTest.log", LogLevel::LOG_DEBUG); }

    static std::unordered_map<std::string, std::string> validConfig() { return {{"SOCKET_HOST", "localhost"}, {"SOCKET_PORT", "9000"}}; }
};
}

TEST_F(TCPSourceConfigTest, MinimalConfigTakesDefaults)
{
    const auto config = TCPSource::validateAndFormat(validConfig());
    EXPECT_EQ(std::get<std::string>(config.at("SOCKET_HOST")), "localhost");
    EXPECT_EQ(std::get<uint32_t>(config.at("SOCKET_PORT")), 9000U);
    EXPECT_EQ(std::get<uint32_t>(config.at("CONNECT_TIMEOUT_MS")), 5000U);
    EXPECT_EQ(std::get<uint32_t>(config.at("FLUSH_INTERVAL_MS")), 100U);
}

TEST_F(TCPSourceConfigTest, PortBoundsAreAccepted)
{
    for (const auto* port : {"1", "65535"})
    {
        auto config = validConfig();
        config["SOCKET_PORT"] = port;
        EXPECT_NO_THROW(TCPSource::validateAndFormat(config)) << "port " << port;
    }
}

TEST_F(TCPSourceConfigTest, PortOutOfRangeIsRejected)
{
    for (const auto* port : {"0", "65536", "70000"})
    {
        SCOPED_TRACE(port);
        auto config = validConfig();
        config["SOCKET_PORT"] = port;
        ASSERT_EXCEPTION_ERRORCODE(TCPSource::validateAndFormat(config), ErrorCode::InvalidConfigParameter);
    }
}

TEST_F(TCPSourceConfigTest, MissingHostIsRejected)
{
    auto config = validConfig();
    config.erase("SOCKET_HOST");
    ASSERT_EXCEPTION_ERRORCODE(TCPSource::validateAndFormat(config), ErrorCode::InvalidConfigParameter);
}

TEST_F(TCPSourceConfigTest, MissingPortIsRejected)
{
    auto config = validConfig();
    config.erase("SOCKET_PORT");
    ASSERT_EXCEPTION_ERRORCODE(TCPSource::validateAndFormat(config), ErrorCode::InvalidConfigParameter);
}

TEST_F(TCPSourceConfigTest, UnknownKeyIsRejected)
{
    auto config = validConfig();
    config["SOCKET_TYPE"] = "SOCK_STREAM";
    ASSERT_EXCEPTION_ERRORCODE(TCPSource::validateAndFormat(config), ErrorCode::InvalidConfigParameter);
}

}
