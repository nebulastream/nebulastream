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

#include <Interpreter/DataValue/Address.hpp>
#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Interpreter {

class ValueTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ValueTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ValueTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ValueTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ValueTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ValueTest test class." << std::endl; }
};

TEST_F(ValueTest, functionCallTest) {
    auto intValue = std::make_unique<Integer>(42);
    std::unique_ptr<Any> valAny = cast<Any>(intValue);
    std::unique_ptr<Any> valAny2 = cast<Any>(valAny);

    auto anyValue = Value<Integer>(std::move(intValue));
    anyValue = anyValue + 10;

    Value<Integer> val = Value<Integer>(42);
    Value<Integer> val2 = 42;
    Value<Any> va = val2;
    Value<Any> va2 = Value<>(10);
    auto anyValueNew1 = Value<>(10);
    auto anyValueNew2 = Value<>(10);
    auto anyValueNew3 = anyValueNew1;
    anyValueNew3 = anyValueNew2;
    val2 = val;
}

TEST_F(ValueTest, addValueTest) {
    auto x = Value<>(1);
    auto y = Value<>(2);
    auto intZ = y + x;
    ASSERT_EQ(intZ.value->value, 3);

    Value<Any> anyZ = y + x;
    ASSERT_EQ(anyZ.as<Integer>().value->value, 3);

    anyZ = intZ + intZ;
    ASSERT_EQ(anyZ.as<Integer>().value->value, 6);
}

}// namespace NES::Interpreter