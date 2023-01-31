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

#ifdef ENABLE_JNI

#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/MapJavaUdfOperatorHandler.hpp>
#include <Execution/Operators/Relational/JVMContext.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
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

};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    explicit MockedPipelineExecutionContext(OperatorHandlerPtr handler)
        : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
          this->buffers.emplace_back(std::move(buffer));
        },
        [this](TupleBuffer& buffer) {
          this->buffers.emplace_back(std::move(buffer));
        },
        {std::move(handler)}){
        // nop
    };

    std::vector<TupleBuffer> buffers;
};

std::string path = std::string(TEST_DATA_DIRECTORY) + "/JavaUdfTestData";
std::string method = "map";
std::unordered_map<std::string, std::vector<char>> byteCodeList;
std::vector<char> serializedInstance;
SchemaPtr input, output;
std::string clazz, inputClass, outputClass;

/**
 * @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, IntegerUDFTest) {
    input = Schema::create()->addField("id", NES::INT32);
    output = Schema::create()->addField("id", NES::INT32);
    clazz = "IntegerMapFunction";
    inputClass = "java/lang/Integer";
    outputClass = "java/lang/Integer";

    auto initialValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with short objects as input and output (IntegerMapFunction<Short, Short>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ShortUDFTest) {
    input = Schema::create()->addField("id", NES::INT16);
    output = Schema::create()->addField("id", NES::INT16);
    clazz = "ShortMapFunction";
    inputClass = "java/lang/Short";
    outputClass = "java/lang/Short";

    auto initialValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with byte objects as input and output (IntegerMapFunction<Byte, Byte>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ByteUDFTest) {
    input = Schema::create()->addField("id", NES::INT8);
    output = Schema::create()->addField("id", NES::INT8);
    clazz = "ByteMapFunction";
    inputClass = "java/lang/Byte";
    outputClass = "java/lang/Byte";

    auto initialValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with long objects as input and output (IntegerMapFunction<Long, Long>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, LongUDFTest) {
    input = Schema::create()->addField("id", NES::INT64);
    output = Schema::create()->addField("id", NES::INT64);
    clazz = "LongMapFunction";
    inputClass = "java/lang/Long";
    outputClass = "java/lang/Long";

    auto initialValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with double objects as input and output (IntegerMapFunction<Double, Double>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, DoubleUDFTest) {
    input = Schema::create()->addField("id", NES::FLOAT64);
    output = Schema::create()->addField("id", NES::FLOAT64);
    clazz = "DoubleMapFunction";
    inputClass = "java/lang/Double";
    outputClass = "java/lang/Double";

    double initialValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10.0);
}

/**
 * @brief Test simple UDF with float objects as input and output (FloatMapFunction<Float, Float>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, FloatUDFTest) {
    input = Schema::create()->addField("id", NES::FLOAT32);
    output = Schema::create()->addField("id", NES::FLOAT32);
    clazz = "FloatMapFunction";
    inputClass = "java/lang/Float";
    outputClass = "java/lang/Float";

    float initialValue = 42.0;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Float>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initialValue + 10.0);
}

/**
 * @brief Test simple UDF with boolean objects as input and output (BooleanMapFunction<Boolean, Boolean>)
 * The UDF sets incoming tuples to false.
*/
TEST_F(MapJavaUdfOperatorTest, BooleanUDFTest) {
    input = Schema::create()->addField("id", NES::BOOLEAN);
    output = Schema::create()->addField("id", NES::BOOLEAN);
    clazz = "BooleanMapFunction";
    inputClass = "java/lang/Boolean";
    outputClass = "java/lang/Boolean";

    auto initialValue = true;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initialValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), false);
}

/**
 * @brief Test simple UDF with string objects as input and output (StringMapFunction<String, String>)
 * The UDF appends incoming tuples the postfix 'appended'.
*/
TEST_F(MapJavaUdfOperatorTest, StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    input = Schema::create()->addField("id", NES::TEXT);
    output = Schema::create()->addField("id", NES::TEXT);
    clazz = "StringMapFunction";
    inputClass = "java/lang/String";
    outputClass = "java/lang/String";

    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) &wc), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Text>("testValue")}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), Value<Text>("testValue_appended"));
}

/**
 * @brief Test simple UDF with loaded java classes as input and output (ComplexMapFunction<ComplexPojo, ComplexPojo>)
 * The UDF sets the bool to false, numerics +10 and appends to strings the postfix 'appended'.
*/
TEST_F(MapJavaUdfOperatorTest, ComplexPojoMapFunction) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    input = Schema::create()->addField("byteVariable", NES::INT8)->addField("shortVariable", NES::INT16)->addField("intVariable", NES::INT32)->addField("longVariable", NES::INT64)->addField("floatVariable", NES::FLOAT32)->addField("doubleVariable", NES::FLOAT64)->addField("stringVariable", NES::TEXT)->addField("booleanVariable", NES::BOOLEAN);
    output = Schema::create()->addField("byteVariable", NES::INT8)->addField("shortVariable", NES::INT16)->addField("intVariable", NES::INT32)->addField("longVariable", NES::INT64)->addField("floatVariable", NES::FLOAT32)->addField("doubleVariable", NES::FLOAT64)->addField("stringVariable", NES::TEXT)->addField("booleanVariable", NES::BOOLEAN);
    clazz = "ComplexPojoMapFunction";
    inputClass = "ComplexPojo";
    outputClass = "ComplexPojo";

    int8_t initialByte = 10;
    int16_t initialShort = 10;
    int32_t initialInt = 10;
    int64_t initialLong = 10;
    float initialFloat = 10.0;
    double initialDouble = 10.0;
    bool initialBool = true;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"byteVariable", Value<>(initialByte)}, {"shortVariable", Value<>(initialShort)}, {"intVariable", Value<>(initialInt)}, {"longVariable", Value<>(initialLong)}, {"floatVariable", Value<>(initialFloat)}, {"doubleVariable", Value<>(initialDouble)}, {"stringVariable", Value<Text>("testValue")}, {"booleanVariable", Value<>(initialBool)}});
    map.execute(ctx, record);

    EXPECT_EQ(record.read("byteVariable"), initialByte + 10);
    EXPECT_EQ(record.read("shortVariable"), initialShort + 10);
    EXPECT_EQ(record.read("intVariable"), initialInt + 10);
    EXPECT_EQ(record.read("longVariable"), initialLong + 10);
    EXPECT_EQ(record.read("floatVariable"), initialFloat + 10.0);
    EXPECT_EQ(record.read("doubleVariable"), initialDouble + 10.0);
    EXPECT_EQ(record.read("stringVariable"), Value<Text>("testValue_appended"));
    EXPECT_EQ(record.read("booleanVariable"), false);
}

/**
* @brief Test UDF with multiple internal dependencies (DummyRichMapFunction<Integer, Integer>)
*/
TEST_F(MapJavaUdfOperatorTest, DependenciesUDFTest) {
    input = Schema::create()->addField("id", NES::INT32);
    output = Schema::create()->addField("id", NES::INT32);
    clazz = "DummyRichMapFunction";
    inputClass = "java/lang/Integer";
    outputClass = "java/lang/Integer";

    auto initalValue = 42;
    auto handler = std::make_shared<MapJavaUdfOperatorHandler>(clazz, method, inputClass, outputClass, byteCodeList, serializedInstance, input, output, path);
    auto map = MapJavaUdf(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext(handler);
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<>(initalValue)}});
    map.execute(ctx, record);
    ASSERT_EQ(record.read("id"), initalValue + 10);
}

}// namespace NES::Runtime::Execution::Operators
#endif // ENABLE_JNI