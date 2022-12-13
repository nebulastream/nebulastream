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

            std::string testDataPath = std::string(TEST_DATA_DIRECTORY) + "/JavaUdfTestData";
};

/**
* @brief Test proper function of serialization
*/
TEST_F(MapJavaUdfOperatorTest, DeserializationTest) {
    std::string path = testDataPath;

    auto initalValue = 42;
    auto map = MapJavaUdf("", "", "", "", { }, { }, Schema::create(), Schema::create(), path);
    jclass clazz = map.getEnvironment()->FindClass("java/lang/Integer");
    auto initMid = map.getEnvironment()->GetMethodID(clazz, "<init>", "(I)V");
    auto obj = map.getEnvironment()->NewObject(clazz, initMid, initalValue); // init Integer object with 1
    auto serialized  = map.serializeInstance(obj);
    auto deserialized = map.deserializeInstance(serialized);
    auto valueMid = map.getEnvironment()->GetMethodID(clazz, "intValue", "()I");
    jint value = map.getEnvironment()->CallIntMethod(deserialized, valueMid);
    ASSERT_EQ((int) value, initalValue);
}

/**
* @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, IntegerUDFTest) {
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::INT32);
    SchemaPtr output = Schema::create()->addField("id", NES::INT32);
    std::string method = "map";
    std::string clazz = "IntegerMapFunction";
    std::string inputProxy = "java/lang/Integer";
    std::string outputProxy = "java/lang/Integer";

    auto initalValue = 42;
    auto map = MapJavaUdf(clazz, method, inputProxy, outputProxy, { }, { }, input, output, path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<>(initalValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initalValue + 10);
}

/**
* @brief Test simple UDF with float objects as input and output (IntegerMapFunction<Float, Float>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, FloatUDFTest) {
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::FLOAT32);
    SchemaPtr output = Schema::create()->addField("id", NES::FLOAT32);
    std::string method = "map";
    std::string clazz = "FloatMapFunction";
    std::string inputProxy = "java/lang/Float";
    std::string outputProxy = "java/lang/Float";

    float initalValue = 42.0;
    auto map = MapJavaUdf(clazz, method, inputProxy, outputProxy, { }, { }, input, output, path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<Float>(initalValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initalValue + 10.0);
}

/**
* @brief Test simple UDF with boolean objects as input and output (IntegerMapFunction<Float, Float>)
 * The UDF sets incoming tuples to false.
*/
TEST_F(MapJavaUdfOperatorTest, BooleanUDFTest) {
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::BOOLEAN);
    SchemaPtr output = Schema::create()->addField("id", NES::BOOLEAN);
    std::string method = "map";
    std::string clazz = "BooleanMapFunction";
    std::string inputProxy = "java/lang/Boolean";
    std::string outputProxy = "java/lang/Boolean";

    auto initalValue = true;
    auto map = MapJavaUdf(clazz, method, inputProxy, outputProxy, { }, { }, input, output, path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<>(initalValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), false);
}

/**
* @brief Test UDF with multiple internal dependencies (DummyRichMapFunction<Integer, Integer>)
*/
TEST_F(MapJavaUdfOperatorTest, DependenciesUDFTest) {
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::INT32);
    SchemaPtr output = Schema::create()->addField("id", NES::INT32);
    std::string method = "map";
    std::string clazz = "DummyRichMapFunction";
    std::string inputProxy = "java/lang/Integer";
    std::string outputProxy = "java/lang/Integer";

    auto initalValue = 42;
    auto map = MapJavaUdf(clazz, method, inputProxy, outputProxy, { }, { }, input, output, path);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>(nullptr));
    auto record = Record({{"id", Value<>(initalValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initalValue + 10);
}

}// namespace NES::Runtime::Execution::Operators