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

            std::string testDataPath = std::string(TEST_DATA_DIRECTORY) + "/JavaUdfTestData";
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
    std::string inputClass = "java/lang/Integer";
    std::string outputClass = "java/lang/Integer";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::INT16);
    SchemaPtr output = Schema::create()->addField("id", NES::INT16);
    std::string method = "map";
    std::string clazz = "ShortMapFunction";
    std::string inputClass = "java/lang/Short";
    std::string outputClass = "java/lang/Short";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::INT8);
    SchemaPtr output = Schema::create()->addField("id", NES::INT8);
    std::string method = "map";
    std::string clazz = "ByteMapFunction";
    std::string inputClass = "java/lang/Byte";
    std::string outputClass = "java/lang/Byte";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::INT64);
    SchemaPtr output = Schema::create()->addField("id", NES::INT64);
    std::string method = "map";
    std::string clazz = "LongMapFunction";
    std::string inputClass = "java/lang/Long";
    std::string outputClass = "java/lang/Long";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::FLOAT64);
    SchemaPtr output = Schema::create()->addField("id", NES::FLOAT64);
    std::string method = "map";
    std::string clazz = "DoubleMapFunction";
    std::string inputClass = "java/lang/Double";
    std::string outputClass = "java/lang/Double";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::FLOAT32);
    SchemaPtr output = Schema::create()->addField("id", NES::FLOAT32);
    std::string method = "map";
    std::string clazz = "FloatMapFunction";
    std::string inputClass = "java/lang/Float";
    std::string outputClass = "java/lang/Float";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::BOOLEAN);
    SchemaPtr output = Schema::create()->addField("id", NES::BOOLEAN);
    std::string method = "map";
    std::string clazz = "BooleanMapFunction";
    std::string inputClass = "java/lang/Boolean";
    std::string outputClass = "java/lang/Boolean";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("id", NES::TEXT);
    SchemaPtr output = Schema::create()->addField("id", NES::TEXT);
    std::string method = "map";
    std::string clazz = "StringMapFunction";
    std::string inputClass = "java/lang/String";
    std::string outputClass = "java/lang/String";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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
    std::string path = testDataPath;
    SchemaPtr input = Schema::create()->addField("byteVariable", NES::INT8)->addField("shortVariable", NES::INT16)->addField("intVariable", NES::INT32)->addField("longVariable", NES::INT64)->addField("floatVariable", NES::FLOAT32)->addField("doubleVariable", NES::FLOAT64)->addField("stringVariable", NES::TEXT)->addField("booleanVariable", NES::BOOLEAN);
    SchemaPtr output = Schema::create()->addField("byteVariable", NES::INT8)->addField("shortVariable", NES::INT16)->addField("intVariable", NES::INT32)->addField("longVariable", NES::INT64)->addField("floatVariable", NES::FLOAT32)->addField("doubleVariable", NES::FLOAT64)->addField("stringVariable", NES::TEXT)->addField("booleanVariable", NES::BOOLEAN);
    std::string method = "map";
    std::string clazz = "ComplexPojoMapFunction";
    std::string inputClass = "ComplexPojo";
    std::string outputClass = "ComplexPojo";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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

    ASSERT_EQ(record.read("byteVariable"), initialByte + 10);
    ASSERT_EQ(record.read("shortVariable"), initialShort + 10);
    ASSERT_EQ(record.read("intVariable"), initialInt + 10);
    ASSERT_EQ(record.read("longVariable"), initialLong + 10);
    ASSERT_EQ(record.read("floatVariable"), initialFloat + 10.0);
    ASSERT_EQ(record.read("doubleVariable"), initialDouble + 10.0);
    ASSERT_EQ(record.read("stringVariable"), Value<Text>("testValue_appended"));
    ASSERT_EQ(record.read("booleanVariable"), false);
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
    std::string inputClass = "java/lang/Integer";
    std::string outputClass = "java/lang/Integer";
    std::unordered_map<std::string, std::vector<char>> byteCodeList;
    std::vector<char> serializedInstance;

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