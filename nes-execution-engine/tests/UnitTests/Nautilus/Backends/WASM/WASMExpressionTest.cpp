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
#include <gtest/gtest.h>
#include <Util/Logger/Logger.hpp>

namespace NES::Nautilus {

class FunctionExecutionTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::Logger::setupLogging("FunctionExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup FunctionExecutionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup TraceTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down TraceTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down TraceTest test class." << std::endl; }
};

int64_t addInt(int64_t x, int64_t y) {
    return x + y;
}

}