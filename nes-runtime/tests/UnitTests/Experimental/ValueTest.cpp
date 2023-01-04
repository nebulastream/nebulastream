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
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class ValueTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ValueTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ValueTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup ValueTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down ValueTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ValueTest test class."); }
};

TEST_F(ValueTest, assignMentTest) {

    auto intV = Int32(42);
    auto intValue = std::make_shared<Int32>(intV);
    std::shared_ptr<Any> valAny = cast<Any>(intValue);
    std::shared_ptr<Any> valAny2 = cast<Any>(valAny);

    auto anyValue = Value<Int32>(std::move(intValue));
    anyValue = anyValue + 10;

    Value<Int8> val = Value<Int8>((int8_t) 42);
    ASSERT_TRUE(val.value->getType()->isInteger());
    Value<Int8> val2 = (int8_t) 42;
    ASSERT_TRUE(val2.value->getType()->isInteger());
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
    ASSERT_EQ(intZ.as<Int32>().value->getValue(), 3);

    Value<Any> anyZ = y + x;
    ASSERT_EQ(anyZ.as<Int32>().value->getValue(), 3);

    anyZ = intZ + intZ;
    ASSERT_EQ(anyZ.as<Int32>().value->getValue(), 6);
}

}// namespace NES::Nautilus