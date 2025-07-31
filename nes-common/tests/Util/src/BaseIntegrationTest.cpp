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
#include <uuid/uuid.h>
#include <BaseIntegrationTest.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#if defined(__linux__)
#endif
namespace NES::Testing
{
namespace detail::uuid
{

std::string generateUUID()
{
    uuid_t bytes;
    std::array<char, 36 + 1> parsed;
    uuid_generate(bytes);
    uuid_unparse(bytes, parsed.data());
    return std::string(parsed.data(), 36);
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
