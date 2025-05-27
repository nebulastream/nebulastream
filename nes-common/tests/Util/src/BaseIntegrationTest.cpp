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

#include <filesystem>
#include <ios>
#include <random>
#include <sstream>
#include <string>
#include <Util/Logger/Logger.hpp>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#if defined(__linux__)
#endif
namespace NES::Testing
{
namespace detail::uuid
{
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string generateUUID()
{
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++)
    {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++)
    {
        ss << dis(gen);
    }
    return ss.str();
}
}

BaseIntegrationTest::BaseIntegrationTest() : testResourcePath(std::filesystem::current_path() / detail::uuid::generateUUID())
{
}

void BaseIntegrationTest::SetUp()
{
    auto expected = false;
    if (setUpCalled.compare_exchange_strong(expected, true))
    {
        NES::Testing::BaseUnitTest::SetUp();
        if (!std::filesystem::exists(testResourcePath))
        {
            std::filesystem::create_directories(testResourcePath);
        }
        else
        {
            std::filesystem::remove_all(testResourcePath);
            std::filesystem::create_directories(testResourcePath);
        }
    }
    else
    {
        NES_ERROR("SetUp called twice in {}", typeid(*this).name());
    }
}

std::filesystem::path BaseIntegrationTest::getTestResourceFolder() const
{
    return testResourcePath;
}

BaseIntegrationTest::~BaseIntegrationTest()
{
    INVARIANT(setUpCalled, "SetUp not called for test {}", typeid(*this).name());
    INVARIANT(tearDownCalled, "TearDown not called for test {}", typeid(*this).name());
}

void BaseIntegrationTest::TearDown()
{
    auto expected = false;
    if (tearDownCalled.compare_exchange_strong(expected, true))
    {
        std::filesystem::remove_all(testResourcePath);
        NES::Testing::BaseUnitTest::TearDown();
        completeTest();
    }
    else
    {
        NES_ERROR("TearDown called twice in {}", typeid(*this).name());
    }
}

}
