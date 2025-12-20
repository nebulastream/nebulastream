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

#include <BaseIntegrationTest.hpp>

#include <filesystem>
#include <ios>
#include <random>
#include <sstream>
#include <string>
#include <nameof.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

#if defined(__linux__)
#endif
namespace NES::Testing
{

BaseIntegrationTest::BaseIntegrationTest() : testResourcePath(std::filesystem::current_path() / UUIDToString(generateUUID()))
{
}

void BaseIntegrationTest::SetUp()
{
    if (auto expected = false; setUpCalled.compare_exchange_strong(expected, true))
    {
        Testing::BaseUnitTest::SetUp();
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
        NES_ERROR("SetUp called twice in {}", NAMEOF_TYPE_EXPR(*this));
    }
}

std::filesystem::path BaseIntegrationTest::getTestResourceFolder() const
{
    return testResourcePath;
}

BaseIntegrationTest::~BaseIntegrationTest()
{
    INVARIANT(setUpCalled, "SetUp not called for test {}", NAMEOF_TYPE_EXPR(*this));
    INVARIANT(tearDownCalled, "TearDown not called for test {}", NAMEOF_TYPE_EXPR(*this));
}

void BaseIntegrationTest::TearDown()
{
    if (auto expected = false; tearDownCalled.compare_exchange_strong(expected, true))
    {
        std::filesystem::remove_all(testResourcePath);
        Testing::BaseUnitTest::TearDown();
        completeTest();
    }
    else
    {
        NES_ERROR("TearDown called twice in {}", NAMEOF_TYPE_EXPR(*this));
    }
}

}
