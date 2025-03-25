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
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <magic_enum/magic_enum.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{

class LogLevelTest : public Testing::BaseUnitTest
{
};

TEST_F(LogLevelTest, testLogLevel)
{
    constexpr auto filePath = "LogLevelTest.log";
    Logger::setupLogging(filePath, NES::LogLevel::LOG_TRACE);

    /// Depending on the cmake configuration for the log level, only log messages with a higher log level than the one set in the cmake
    /// configuration will be written to the log file.
    NES_TRACE("{}", magic_enum::enum_name(LogLevel::LOG_TRACE));
    NES_DEBUG("{}", magic_enum::enum_name(LogLevel::LOG_DEBUG));
    NES_INFO("{}", magic_enum::enum_name(LogLevel::LOG_INFO));
    NES_WARNING("{}", magic_enum::enum_name(LogLevel::LOG_WARNING));
    NES_ERROR("{}", magic_enum::enum_name(LogLevel::LOG_ERROR));
    NES_FATAL_ERROR("{}", magic_enum::enum_name(LogLevel::LOG_FATAL_ERROR));

    /// We need to call shutdown, otherwise, the logger might not have written the data to the file
    Logger::getInstance()->shutdown();

    /// Reading the log file and checking if the log messages are written correctly.
    std::stringstream logFile;
    logFile << std::ifstream(filePath).rdbuf();
    const std::string logFileContent = logFile.str();

    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_TRACE))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_TRACE)));
    }
    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_DEBUG))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_DEBUG)));
    }
    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_INFO))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_INFO)));
    }
    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_WARNING))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_WARNING)));
    }
    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_ERROR))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_ERROR)));
    }
    if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= NES::getLogLevel(LogLevel::LOG_FATAL_ERROR))
    {
        EXPECT_THAT(logFileContent, testing::HasSubstr(magic_enum::enum_name(LogLevel::LOG_FATAL_ERROR)));
    }
}

}
