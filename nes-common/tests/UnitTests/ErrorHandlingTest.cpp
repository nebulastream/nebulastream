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

#include <ErrorHandling.hpp>

#include <fstream>
#include <sstream>
#include <string>
#include <string_view>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <cpptrace/from_current.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{

namespace
{
/// cpptrace's default formatter opens every non-empty trace with this header.
constexpr std::string_view stackTraceHeader = "Stack trace (most recent call first):";

std::string readLogFile(const std::string& logFileName)
{
    Logger::getInstance()->shutdown();
    std::stringstream logFile;
    logFile << std::ifstream(logFileName).rdbuf();
    return logFile.str();
}
}

TEST(ErrorHandlingTest, tryLogCurrentExceptionKeepsStacktraceForInternalOrigin)
{
#ifndef NES_LOG_WITH_STACKTRACE
    GTEST_SKIP() << "stacktraces are compiled out (NES_LOG_WITH_STACKTRACE not set)";
#endif
    constexpr auto logFileName = "ErrorHandlingConfigOriginInternalTest.log";
    Logger::setupLogging(logFileName, LogLevel::LOG_DEBUG, false);

    CPPTRACE_TRY
    {
        throw InvalidConfigParameter();
    }
    CPPTRACE_CATCH(...)
    {
        tryLogCurrentException(ConfigErrorOrigin::Internal);
    }

    const auto content = readLogFile(logFileName);
    EXPECT_THAT(content, testing::HasSubstr("invalid config parameter"));
    EXPECT_THAT(content, testing::HasSubstr(stackTraceHeader));
}

TEST(ErrorHandlingTest, tryLogCurrentExceptionSuppressesStacktraceForUserInputInvalidConfigParameter)
{
#ifndef NES_LOG_WITH_STACKTRACE
    GTEST_SKIP() << "stacktraces are compiled out (NES_LOG_WITH_STACKTRACE not set)";
#endif
    constexpr auto logFileName = "ErrorHandlingConfigOriginUserInputInvalidConfigTest.log";
    Logger::setupLogging(logFileName, LogLevel::LOG_DEBUG, false);

    CPPTRACE_TRY
    {
        throw InvalidConfigParameter();
    }
    CPPTRACE_CATCH(...)
    {
        tryLogCurrentException(ConfigErrorOrigin::UserInput);
    }

    const auto content = readLogFile(logFileName);
    EXPECT_THAT(content, testing::HasSubstr("invalid config parameter"));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr(stackTraceHeader)));
}

TEST(ErrorHandlingTest, tryLogCurrentExceptionSuppressesStacktraceForUserInputCannotLoadConfig)
{
#ifndef NES_LOG_WITH_STACKTRACE
    GTEST_SKIP() << "stacktraces are compiled out (NES_LOG_WITH_STACKTRACE not set)";
#endif
    constexpr auto logFileName = "ErrorHandlingConfigOriginUserInputCannotLoadConfigTest.log";
    Logger::setupLogging(logFileName, LogLevel::LOG_DEBUG, false);

    CPPTRACE_TRY
    {
        throw CannotLoadConfig();
    }
    CPPTRACE_CATCH(...)
    {
        tryLogCurrentException(ConfigErrorOrigin::UserInput);
    }

    const auto content = readLogFile(logFileName);
    EXPECT_THAT(content, testing::HasSubstr("cannot load config"));
    EXPECT_THAT(content, testing::Not(testing::HasSubstr(stackTraceHeader)));
}

TEST(ErrorHandlingTest, tryLogCurrentExceptionKeepsStacktraceForUserInputOnUnrelatedCode)
{
#ifndef NES_LOG_WITH_STACKTRACE
    GTEST_SKIP() << "stacktraces are compiled out (NES_LOG_WITH_STACKTRACE not set)";
#endif
    constexpr auto logFileName = "ErrorHandlingConfigOriginUserInputUnrelatedCodeTest.log";
    Logger::setupLogging(logFileName, LogLevel::LOG_DEBUG, false);

    CPPTRACE_TRY
    {
        throw TestException("unrelated failure");
    }
    CPPTRACE_CATCH(...)
    {
        tryLogCurrentException(ConfigErrorOrigin::UserInput);
    }

    const auto content = readLogFile(logFileName);
    EXPECT_THAT(content, testing::HasSubstr("unrelated failure"));
    EXPECT_THAT(content, testing::HasSubstr(stackTraceHeader));
}

}
