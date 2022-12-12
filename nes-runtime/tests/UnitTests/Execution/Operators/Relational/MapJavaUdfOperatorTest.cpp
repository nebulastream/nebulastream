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
* @brief Test proper functioning of serialization
*/
TEST_F(MapJavaUdfOperatorTest, DeserializationTest) {
    std::string path = "/home/amichalke/workspace/nebulastream/nebulastream-java-client/";

    auto intialValue = 42;
    auto map = MapJavaUdf("", "", "", "", { }, { }, Schema::create(), Schema::create(), path);
    jclass clazz = map.getEnvironment()->FindClass("java/lang/Integer");
    auto initMid = map.getEnvironment()->GetMethodID(clazz, "<init>", "(I)V");
    auto obj = map.getEnvironment()->NewObject(clazz, initMid, intialValue); // init Integer object with 1
    auto serialized  = map.serializeInstance(obj);
    auto deserialized = map.deserializeInstance(serialized);
    auto valueMid = map.getEnvironment()->GetMethodID(clazz, "intValue", "()I");
    jint value = map.getEnvironment()->CallIntMethod(deserialized, valueMid);
    ASSERT_EQ((int) value, intialValue);
}

/**
* @brief test default class loading
*/
TEST_F(MapJavaUdfOperatorTest, ClassLoaderTest) {
    std::string path = "/home/amichalke/workspace/nebulastream/nebulastream-java-client/";
    SchemaPtr input = Schema::create();
    SchemaPtr output = Schema::create();
    std::string method = "map"; // add pkg?
    std::string clazz = "DummyRichMapFunction"; // add pkg
    std::string inputProxy = "java/lang/Integer";
    std::string outputProxy = "java/lang/Integer";

    auto map = MapJavaUdf(clazz, method, inputProxy, outputProxy, { }, { }, input, output, path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<>(10)}, {"value", Value<>(12)}});
    map.execute(ctx, record);
}

/**
* @brief Test custom class loader
*/
TEST_F(MapJavaUdfOperatorTest, CustomClassLoaderTest) {
}
}// namespace NES::Runtime::Execution::Operators