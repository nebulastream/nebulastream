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

#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class SinkConfigTest : public Testing::BaseIntegrationTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SinkConfigTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup Sink Config test class.");
    }
};

struct TestSinkConfig
{
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "FILE_PATH",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<bool> APPEND{
        "APPEND",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(APPEND, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH, APPEND);
};

TEST_F(SinkConfigTest, ShouldListAcceptedParametersForUnknownParameter)
{
    try
    {
        DescriptorConfig::validateAndFormat<TestSinkConfig>({{"FILEPATH", "/tmp/probe.csv"}}, "File");
        FAIL() << "Expected an InvalidConfigParameter exception";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::InvalidConfigParameter);
        EXPECT_EQ(
            std::string{exception.what()},
            "invalid config parameter; Unknown configuration parameter: FILEPATH. Accepted parameters for File are: APPEND, FILE_PATH.\n");
    }
}

TEST_F(SinkConfigTest, ShouldAcceptKnownParameters)
{
    const auto config = DescriptorConfig::validateAndFormat<TestSinkConfig>({{"FILE_PATH", "/tmp/probe.csv"}}, "File");
    EXPECT_EQ(std::get<std::string>(config.at("FILE_PATH")), "/tmp/probe.csv");
    EXPECT_FALSE(std::get<bool>(config.at("APPEND")));
}
}
