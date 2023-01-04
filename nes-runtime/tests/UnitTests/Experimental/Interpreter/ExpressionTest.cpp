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
#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
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

namespace NES::Nautilus {

class ExpressionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ExpressionTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup ExpressionTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down ExpressionTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down ExpressionTest test class."); }
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

}// namespace NES::Nautilus