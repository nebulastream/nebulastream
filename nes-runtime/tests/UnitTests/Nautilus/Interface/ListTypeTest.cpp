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
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class ListTypeTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ListTypeTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ListTypeTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
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
    ASSERT_EQ(length, (uint32_t) 10);
    Value<> any = valueList;
    Value<List> l = any.as<List>();
    ASSERT_EQ(valueList, l);
    auto res2 = (valueList->equals(l));
    auto isList2 = dynamic_cast<List*>(&any.getValue());
    auto isList = any->isType<TypedList<Int32>>();
    ASSERT_TRUE(isList);
}

TEST_F(ListTypeTest, createListTypeFromArray) {
    int32_t array[6] = {0, 1, 2, 3, 4, 5};
    auto list = ListValue<int32_t>::create(array, 6);
    for (auto i = 0; i < 6; i++) {
        ASSERT_EQ(array[i], i);
    }
    // free list value explicitly here.
    list->~ListValue<int32_t>();
}

}// namespace NES::Nautilus