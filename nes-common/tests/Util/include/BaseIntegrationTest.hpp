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
#pragma once

#include <filesystem>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <BaseUnitTest.hpp>

#define ASSERT_INSTANCE_OF(node, instance) \
    if (!(node)->instanceOf<instance>()) \
    { \
        auto message = (node)->toString() + " is not of instance " + std::string(typeid(instance).name()); \
        GTEST_FATAL_FAILURE_(message.c_str()); \
    }

namespace NES::Testing
{

class BaseIntegrationTest : public Testing::BaseUnitTest
{
public:
    /**
     * @brief the base test class ctor that creates the internal test resources
     */
    explicit BaseIntegrationTest();

    ~BaseIntegrationTest() override;

    /**
     * @brief Fetches the port
     */
    void SetUp() override;

    /**
     * @brief Release internal ports
     */
    void TearDown() override;

protected:
    /**
     * @brief returns the test resource folder to write files
     * @return the test folder
     */
    std::filesystem::path getTestResourceFolder() const;

private:
    std::filesystem::path testResourcePath;
    std::atomic<bool> setUpCalled{false};
    std::atomic<bool> tearDownCalled{false};
};
}
