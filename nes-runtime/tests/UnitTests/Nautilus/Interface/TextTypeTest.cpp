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

#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class TextTypeTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TextTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TextTypeTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
        NES_DEBUG("Setup TextTypeTest test case.")
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_DEBUG("Tear down TextTypeTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down TextTypeTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(TextTypeTest, createTextTest) {

    auto textValue = Value<Text>("test");
    auto length = textValue->length();
    ASSERT_EQ(length, (uint32_t) 4);

    auto textValue2 = Value<Text>("test");
    ASSERT_EQ(textValue, textValue2);

    auto textValue3 = Value<Text>("teso");
    ASSERT_NE(textValue, textValue3);

    Value<> character = textValue[0];
    ASSERT_EQ(character, 't');

    textValue[3] = (int8_t) 'o';
    character = textValue[3];
    ASSERT_EQ(character, 'o');
    ASSERT_EQ(textValue, textValue3);

    for (Value<UInt32> i = (uint32_t) 0; i < textValue->length(); i = i + (uint32_t) 1) {
        textValue[i] = (int8_t) 'z';
    }
    auto textValue4 = Value<Text>("zzzz");
    ASSERT_EQ(textValue, textValue4);
}

}// namespace NES::Nautilus