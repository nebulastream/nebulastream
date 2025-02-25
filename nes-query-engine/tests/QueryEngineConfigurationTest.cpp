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

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <QueryEngineConfiguration.hpp>

namespace NES::Testing
{
class QueryEngineConfigurationTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("QueryEngineConfigurationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryEngineConfigurationTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

TEST_F(QueryEngineConfigurationTest, testConfigurationsDefault)
{
    Runtime::QueryEngineConfiguration const defaultConfig;
    EXPECT_EQ(defaultConfig.taskQueueSize.getValue(), 1000);
    EXPECT_EQ(defaultConfig.numberOfWorkerThreads.getValue(), 4);
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsValidInput)
{
    Runtime::QueryEngineConfiguration defaultConfig;
    defaultConfig.overwriteConfigWithCommandLineInput({{"taskQueueSize", "200"}, {"numberOfWorkerThreads", "2"}});

    EXPECT_EQ(defaultConfig.taskQueueSize.getValue(), 200);
    EXPECT_EQ(defaultConfig.numberOfWorkerThreads.getValue(), 2);
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsBadInputNonString)
{
    Runtime::QueryEngineConfiguration defaultConfig;
    EXPECT_ANY_THROW(defaultConfig.overwriteConfigWithCommandLineInput({{"taskQueueSize", "XX"}, {"numberOfWorkerThreads", "2"}}));

    Runtime::QueryEngineConfiguration defaultConfig1;
    EXPECT_ANY_THROW(defaultConfig1.overwriteConfigWithCommandLineInput({{"taskQueueSize", "200"}, {"numberOfWorkerThreads", "XX"}}));

    Runtime::QueryEngineConfiguration defaultConfig2;
    EXPECT_ANY_THROW(defaultConfig2.overwriteConfigWithCommandLineInput({{"taskQueueSize", "XX"}, {"numberOfWorkerThreads", "XX"}}));

    Runtime::QueryEngineConfiguration defaultConfig3;
    EXPECT_ANY_THROW(defaultConfig2.overwriteConfigWithCommandLineInput({{"taskQueueSize", "1.0"}, {"numberOfWorkerThreads", "1.5"}}));
}

TEST_F(QueryEngineConfigurationTest, testConfigurationsBadInputBadNumberOfThreads)
{
    Runtime::QueryEngineConfiguration defaultConfig;
    EXPECT_ANY_THROW(defaultConfig.overwriteConfigWithCommandLineInput({{"taskQueueSize", "200"}, {"numberOfWorkerThreads", "0"}}));

    Runtime::QueryEngineConfiguration const defaultConfig1;
    EXPECT_ANY_THROW(defaultConfig.overwriteConfigWithCommandLineInput({{"taskQueueSize", "200"}, {"numberOfWorkerThreads", "20000"}}));
}

}
