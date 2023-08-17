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

#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Interface/DataTypes/Float/Float.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>
#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/DataTypes/List/ListValue.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class ListTypeTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ListTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ListTypeTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling::SetUp();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
        NES_DEBUG("Setup ListTypeTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down ListTypeTest test class."); }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(ListTypeTest, createListTest) {
    auto list = ListValue<int32_t>::create(10);
    auto listRef = TypedRef<ListValue<int32_t>>(list);
    auto valueList = Value<TypedList<Int32>>(TypedList<Int32>(listRef));
    auto length = valueList->length();
    ASSERT_EQ(length, 10_u32);
    Value<> any = valueList;
    Value<List> l = any.as<List>();
    ASSERT_EQ(valueList, l);
    auto res2 = (valueList->equals(l));
    auto isList = any->isType<TypedList<Int32>>();
    ASSERT_TRUE(isList);
}

TEST_F(ListTypeTest, createListTypeFromArray) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list = ListValue<int32_t>::create(array, 6);
    for (auto i = 0; i < 6; i++) {
        ASSERT_EQ(list->data()[i], i);
    }
    // free list value explicitly here.
    list->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, concatTest) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list1 = ListValue<int32_t>::create(array, 6);
    auto list2 = ListValue<int32_t>::create(array, 6);

    auto result = list1->concat(list2);
    ASSERT_EQ(result->length(), 12);
    for (int32_t i = 0; i < 12; i++) {
        ASSERT_EQ(result->data()[i], i % 6);
    }
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
    list2->~ListValue<int32_t>();
    result->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, prepend) {
    int32_t array[7] = {1, 2, 3, 4, 5, 6, 7};
    auto list1 = ListValue<int32_t>::create(array, 7);
    auto result = list1->prepend(10);
    ASSERT_EQ(result->data()[0], 10);
    ASSERT_EQ(result->length(), 8);
    for (int32_t i = 1; i < 8; i++) {
        EXPECT_EQ(result->data()[i], i);
    }
    list1->~ListValue<int32_t>();
    result->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, listPosition) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list1 = ListValue<int32_t>::create(array, 6);
    ASSERT_EQ(list1->listPosition(2), 2);
    ASSERT_EQ(list1->listPosition(5), 5);
    ASSERT_EQ(list1->listPosition(0), 0);
    ASSERT_EQ(list1->listPosition(7), -1);
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, reverse) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list1 = ListValue<int32_t>::create(array, 6);
    auto result = list1->revers();
    ASSERT_EQ(result->length(), 6);
    ASSERT_EQ(result->data()[0], 5);
    ASSERT_EQ(result->data()[1], 4);
    ASSERT_EQ(result->data()[2], 3);
    ASSERT_EQ(result->data()[3], 2);
    ASSERT_EQ(result->data()[4], 1);
    ASSERT_EQ(result->data()[5], 0);
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
    result->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, appendTest) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list1 = ListValue<int32_t>::create(array, 6);
    auto result = list1->append(6);
    ASSERT_EQ(result->length(), 7);
    for (int32_t i = 0; i < 7; i++) {
        ASSERT_EQ(result->data()[i], i);
    }
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
    result->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, containsTest) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list1 = ListValue<int32_t>::create(array, 6);
    ASSERT_EQ(list1->contains(3), true);
    ASSERT_EQ(list1->contains(6), false);
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
}

TEST_F(ListTypeTest, sortTest) {
    int32_t array[6] = {6, 3, 5, 1, 4, 2};
    auto list1 = ListValue<int32_t>::create(array, 6);

    auto result = list1->sort();
    ASSERT_EQ(result->length(), 6);
    for (int32_t i = 1; i < 6; i++) {
        EXPECT_EQ(result->data()[i - 1], i);
    }
    // free each list value explicitly here.
    list1->~ListValue<int32_t>();
    result->~ListValue<int32_t>();
}

}// namespace NES::Nautilus