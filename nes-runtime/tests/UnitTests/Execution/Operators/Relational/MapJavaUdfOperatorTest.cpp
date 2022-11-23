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

#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Expressions/WriteFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {
class MapJavaUdfOperatorTest : public testing::Test {
          public:
            /* Will be called before any test in this class are executed. */
            static void SetUpTestCase() {
                NES::Logger::setupLogging("MapJavaUdfOperatorTest.log", NES::LogLevel::LOG_DEBUG);
                std::cout << "Setup MapJavaUdfOperatorTest test class." << std::endl;
            }

            /* Will be called before a test is executed. */
            void SetUp() override { std::cout << "Setup MapJavaUdfOperatorTest test case." << std::endl; }

            /* Will be called before a test is executed. */
            void TearDown() override { std::cout << "Tear down MapJavaUdfOperatorTest test case." << std::endl; }

            /* Will be called after all tests in this class are finished. */
            static void TearDownTestCase() { std::cout << "Tear down MapJavaUdfOperatorTest test class." << std::endl; }
};

/**
* @brief Tests test 123
*/
TEST_F(MapJavaUdfOperatorTest, ClassLoaderTest) {
    const std::string path = "/home/amichalke/nebulastream/udf";
    auto map = MapJavaUdf(path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<>(10)}, {"value", Value<>(12)}});
    map.execute(ctx, record);
}
}// namespace NES::Runtime::Execution::Operators