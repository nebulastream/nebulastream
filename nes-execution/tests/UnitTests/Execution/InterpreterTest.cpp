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

#include <Interpreter/DataValue/Integer.hpp>
#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Operations/AddOp.hpp>
#include <Util/Logger/Logger.hpp>
#include <execinfo.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Interpreter {

class InterpreterTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("InterpreterTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup InterpreterTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup InterpreterTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down InterpreterTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down InterpreterTest test class." << std::endl; }
};

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(InterpreterTest, valueTest) {
    std::unique_ptr<Any> i1 = Integer::create<Integer>(1);
    std::unique_ptr<Any> i2 = Integer::create<Integer>(2);

    // auto t1 = TraceValue::create<TraceValue>(std::move(i1));
    // auto t2 = TraceValue::create<TraceValue>(std::move(i2));
    Value iw = Value(i1);
    Value iw2 = Value(i2);

    //auto result = t1 + t2;

    //auto res = (iw + iw2);
    auto res2 = (iw + iw2) + iw2 + 2;

    if (res2 <= 7) {
        std::cout << "go in" << std::endl;
    } else {
        std::cout << "go out" << std::endl;
    }

    // ASSERT_EQ(cast<Integer>(result)->getValue(), 3);
}

TEST_F(InterpreterTest, valueTestLoop) {
    std::unique_ptr<Any> i1 = Integer::create<Integer>(1);
    std::unique_ptr<Any> i2 = Integer::create<Integer>(2);

    // auto t1 = TraceValue::create<TraceValue>(std::move(i1));
    // auto t2 = TraceValue::create<TraceValue>(std::move(i2));
    Value iw = Value(i1);
    Value iw2 = Value(i2);

    void *buffer[20];
    // First add the RIP pointers
    int backtrace_size = backtrace(buffer, 20);

    auto res = __builtin_return_address(0);
    auto res1 = __builtin_return_address(1);
    auto sp = reinterpret_cast<void**>(__builtin_frame_address(0));


    //auto result = t1 + t2;

    //auto res = (iw + icw2);
    for (auto start = iw; start < 10; start = start + 1) {
        std::cout << "loop" << std::endl;
    }



    // ASSERT_EQ(cast<Integer>(result)->getValue(), 3);
}

}// namespace NES::Interpreter