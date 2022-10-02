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
#include <Experimental/Interpreter/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
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

namespace NES::ExecutionEngine::Experimental::Interpreter {

class ExpressionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ExpressionTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override { std::cout << "Setup ExpressionTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down ExpressionTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ExpressionTest test class." << std::endl; }
};

TEST_F(ExpressionTest, EqualsExpressionInteger) {
    auto readField1 = std::make_shared<ReadFieldExpression>("f1");
    auto readField2 = std::make_shared<ReadFieldExpression>("f2");
    auto equalsExpression = std::make_shared<EqualsExpression>(readField1, readField2);

    auto r1 = Record({{"f1", Value<Int32>(1)}, {"f2", Value<Int32>(1)}});
    ASSERT_TRUE(equalsExpression->execute(r1).as<Boolean>()->getValue());
}

TEST_F(ExpressionTest, ExpressionReadInvalidField) {
    auto readField1 = std::make_shared<ReadFieldExpression>("f3");
    auto r1 = Record({{"f1", Value<Int32>(1)}, {"f2", Value<Int32>(1)}});
    ASSERT_ANY_THROW(readField1->execute(r1));
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter