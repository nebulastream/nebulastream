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
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class TextTypeTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TextTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TextTypeTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
        NES_DEBUG("Setup TextTypeTest test case.")
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down TextTypeTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(TextTypeTest, createTextTest) {
    auto textValue = Value<Text>("test");
    auto length = textValue->length();
    EXPECT_EQ(length, 4_u32);

    auto textValue2 = Value<Text>("test");
    EXPECT_EQ(textValue, textValue2);

    auto textValue3 = Value<Text>("teso");
    ASSERT_NE(textValue, textValue3);

    Value<> character = textValue[0];
    EXPECT_EQ(character, 't');

    textValue[3] = (int8_t) 'o';
    character = textValue[3];
    EXPECT_EQ(character, 'o');
    EXPECT_EQ(textValue, textValue3);

    for (Value<UInt32> i = 0_u32; i < textValue->length(); i = i + 1_u32) {
        textValue[i] = (int8_t) 'z';
    }
    auto textValue4 = Value<Text>("zzzz");
    EXPECT_EQ(textValue, textValue4);

    auto bitLength = textValue->bitLength();
    EXPECT_EQ(bitLength, 32_u32);
}

TEST_F(TextTypeTest, LowerUpperTest) {
    auto LowerUpperTest1 = Value<Text>("test");
    auto LowerUpperTest2 = LowerUpperTest1->upper();
    auto LowerUpperTest3 = Value<Text>("TEST");
    EXPECT_EQ(LowerUpperTest2, LowerUpperTest3);

    auto LowerUpperTest4 = LowerUpperTest3->lower();
    EXPECT_EQ(LowerUpperTest4, LowerUpperTest1);
}

TEST_F(TextTypeTest, LeftRightTest) {
    auto LeftRightTest1 = Value<Text>("Test");
    auto LeftRightTest2 = Value<Text>("Te");
    auto LeftRightTest3 = LeftRightTest2->left(2_u32);
    EXPECT_EQ(LeftRightTest3, LeftRightTest2);

    auto LeftRightTest4 = Value<Text>("st");
    auto LeftRightTest5 = LeftRightTest1->right(2_u32);
    EXPECT_EQ(LeftRightTest5, LeftRightTest4);
}

TEST_F(TextTypeTest, PadTest) {
    auto PadTest1 = Value<Text>("Test");
    auto PadTest2 = Value<Text>("Testoo");
    auto PadTest3 = PadTest1->rpad(6_u32, (int8_t) 'o');
    EXPECT_EQ(PadTest2, PadTest3);
    auto PadTest4 = Value<Text>("ooTest");
    auto PadTest5 = PadTest1->lpad(6_u32, (int8_t) 'o');
    EXPECT_EQ(PadTest4, PadTest5);
}

TEST_F(TextTypeTest, TrimTest) {
    auto TrimSpaceTest = Value<Text>("  Test");
    auto TrimTest0 = Value<Text>("Test");
    auto TrimTest1 = Value<Text>("  Test");
    auto TrimTest2 = TrimTest1->ltrim(TrimTest0);
    EXPECT_EQ(TrimTest0, TrimTest2);

    auto TrimTest3 = Value<Text>("Test  ");
    auto TrimTest4 = TrimTest3->rtrim(TrimTest0);
    EXPECT_EQ(TrimTest4, TrimTest0);

    auto Trimtest5 = Value<Text>("  Test  ");
    auto TrimTest6 = Trimtest5->trim();
    EXPECT_EQ(TrimTest6, TrimTest0);

    auto TrimTest7 = TrimSpaceTest->ltrim(TrimTest0);
    TrimTest7 = TrimTest7->rtrim(TrimTest0);
    EXPECT_EQ(TrimTest7, TrimTest0);
}

TEST_F(TextTypeTest, prefixTest) {
    auto preTest1 = Value<Text>("abc");
    auto preTest2 = Value<Text>("ab");
    bool x = preTest1->prefix(preTest2);
    EXPECT_EQ(x, true);
    EXPECT_ANY_THROW(preTest2->prefix(preTest1));
}

TEST_F(TextTypeTest, repeatTest) {
    auto repeatTest1 = Value<Text>("A");
    auto repeatTest2 = repeatTest1->repeat(5_u32);
    auto repeatTest3 = Value<Text>("AAAAA");
    EXPECT_EQ(repeatTest2, repeatTest3);
    EXPECT_ANY_THROW(repeatTest1->repeat(0_u32));
}

TEST_F(TextTypeTest, reverseTest) {
    auto reverseTest1 = Value<Text>("hello");
    auto reverseTest2 = reverseTest1->reverse();
    auto reverseTest3 = Value<Text>("olleh");
    EXPECT_EQ(reverseTest2, reverseTest3);
}

TEST_F(TextTypeTest, positionTest) {
    auto positionTest1 = Value<Text>("Nebula");
    auto positionTest2 = Value<Text>("NNebulaStream");
    auto positionTest3 = Value<Text>("Streamm");
    auto positionTest4 = Value<Text>("e");
    auto test1 = positionTest2->position(positionTest1);
    auto test2 = positionTest2->position(positionTest4);
    auto test3 = positionTest2->position(positionTest3);
    EXPECT_EQ(test1, 2_u32);
    EXPECT_EQ(test2, 3_u32);
    EXPECT_EQ(test3, 0_u32);
    EXPECT_ANY_THROW(positionTest1->position(positionTest2));
}

TEST_F(TextTypeTest, replaceTest) {
    auto replaceTest1 = Value<Text>("xoldxold");
    auto replaceTest2 = Value<Text>("old");
    auto replaceTest3 = Value<Text>("new");
    auto replaceTest4 = Value<Text>("xnewxnew");
    auto test1 = replaceTest1->replace(replaceTest2, replaceTest3);
    EXPECT_EQ(test1, replaceTest4);
    auto test2 = replaceTest2->replace(replaceTest3, replaceTest4);
    EXPECT_EQ(test2, replaceTest2);
}

TEST_F(TextTypeTest, subStringTest) {
    auto subtext1 = Value<Text>("Hello");
    auto subtext2 = subtext1->substring(2_u32, 2_u32);
    auto subtext3 = Value<Text>("el");
    EXPECT_EQ(subtext2, subtext3);
}

TEST_F(TextTypeTest, subStringTestFail) {
    auto subtext1 = Value<Text>("Hello");
    EXPECT_ANY_THROW(subtext1->substring(200_u32, 200_u32));
}

TEST_F(TextTypeTest, stringconcatTest) {
    auto concatTest1 = Value<Text>("Nebula");
    auto concatTest2 = Value<Text>("Stream");
    auto concatTest3 = Value<Text>("NebulaStream");
    auto concatTest5 = concatTest1->concat(concatTest2);
    EXPECT_EQ(concatTest5, concatTest3);
}

TEST_F(TextTypeTest, similarToTest) {
    auto similarToTest1 = Value<Text>("NebulaStream");
    auto similarToTest2 = Value<Text>("NebulaStream");
    auto similarToResult = similarToTest1->similarTo(similarToTest2);
    EXPECT_EQ(similarToResult, (Boolean) true);
    EXPECT_TRUE(similarToResult->getTypeIdentifier()->isType<Boolean>());
}

TEST_F(TextTypeTest, similarToTestFail) {
    auto similarToTest1 = Value<Text>("NebulaStream");
    auto similarToTest2 = Value<Text>("Nebula");
    auto similarToResult = similarToTest1->similarTo(similarToTest2);
    EXPECT_EQ(similarToResult, (Boolean) false);
    EXPECT_TRUE(similarToResult->getTypeIdentifier()->isType<Boolean>());
}

TEST_F(TextTypeTest, likeTest) {
    auto likeTest1 = Value<Text>("abcde");
    auto likeTest2 = Value<Text>("%bcd%");
    auto caseSensitiveFlag = Value<Boolean>(false);
    auto likeResult = likeTest1->like(likeTest2, caseSensitiveFlag);
    EXPECT_EQ(likeResult, (Boolean) true);
    EXPECT_TRUE(likeResult->getTypeIdentifier()->isType<Boolean>());
}

TEST_F(TextTypeTest, likeTestFalse) {
    auto likeTest1 = Value<Text>("abcde");
    auto likeTest2 = Value<Text>("_bc_");
    auto caseSensitiveFlag = Value<Boolean>(false);
    auto likeResult = likeTest1->like(likeTest2, caseSensitiveFlag);
    EXPECT_EQ(likeResult, (Boolean) false);
    EXPECT_TRUE(likeResult->getTypeIdentifier()->isType<Boolean>());
}

TEST_F(TextTypeTest, extractStringValueFromTupleBuffer) {
    // Create a tupleBuffer with a string that is 10 chars long
    const char* buf = "1234567890";
    auto tupleBuffer = bm->getBufferBlocking();
    *tupleBuffer.getBuffer<uint32_t>() = std::strlen(buf) + 1;
    std::strncpy(tupleBuffer.getBuffer<char>() + sizeof(uint32_t), buf, std::strlen(buf) + 1);
    // Create a text value from this string that's only 5 chars long
    auto textValue = NES::Nautilus::TextValue::create(tupleBuffer, 5_u32);
    // Call strn_copy to create a null terminated string of the text value.
    const char* expected = "12345";
    auto actual = textValue->strn_copy();
    NES_DEBUG("expected = {}, actual = {}", expected, actual);
    EXPECT_EQ(strcmp(expected, actual.data()), 0);
    // call the destructor on the text value to free the underling tuple buffer.
    textValue->~TextValue();
}

}// namespace NES::Nautilus