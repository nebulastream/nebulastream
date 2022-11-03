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
#include <Nautilus/Interface/DataTypes/List.hpp>
#include <Nautilus/Interface/DataTypes/TypedRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Nautilus {

class ListTypeTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ListTypeTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ListTypeTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<Runtime::WorkerContext>(0, bm, 100);
        std::cout << "Setup ListTypeTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ListTypeTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ListTypeTest test class." << std::endl; }
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<Runtime::WorkerContext> wc;
};

TEST_F(ListTypeTest, createListTest) {
    auto list = RawList(10);
    auto listRef = TypedRef<RawList>(list);
    auto valueList = Value<TypedList<Int32>>(TypedList<Int32>(listRef));
    auto length = valueList->length();
    ASSERT_EQ(length, 10);
}

}// namespace NES::Nautilus