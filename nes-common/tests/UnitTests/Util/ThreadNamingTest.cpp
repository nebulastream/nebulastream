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

#include <array>
#include <cstring>
#include <string_view>
#include <pthread.h>
#include <unistd.h>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
class ThreadNamingTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("ThreadNamingTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("ThreadNamingTest test class SetUpTestCase.");
    }
};

TEST_F(ThreadNamingTest, testThreadNaming)
{
    setThreadName("NES-0");

    std::array<char, detail::PTHREAD_NAME_LENGTH + 1> pthreadName{};
    pthread_getname_np(pthread_self(), pthreadName.data(), pthreadName.size());

    EXPECT_EQ(std::string_view(pthreadName.data()), "NES-0");
}

TEST_F(ThreadNamingTest, testThreadNamingWithTruncation)
{
    setThreadName("NES_LONG-123456789");
    std::array<char, detail::PTHREAD_NAME_LENGTH + 1> pthreadName{};
    pthread_getname_np(pthread_self(), pthreadName.data(), pthreadName.size());

    EXPECT_EQ(std::string_view(pthreadName.data()), "NES_LONG-123456");
}
}
