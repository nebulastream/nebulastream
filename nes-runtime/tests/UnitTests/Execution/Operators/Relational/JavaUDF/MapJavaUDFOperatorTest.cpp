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

#include <API/Schema.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
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

std::string method = "map";
jni::JavaUDFByteCodeList byteCodeList;
jni::JavaSerializedInstance serializedInstance;
SchemaPtr input, output;
std::string clazz, inputClass, outputClass;

/**
 * @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, IntegerUDFTest) {
    input = Schema::create()->addField("id", BasicType::INT32);
    output = Schema::create()->addField("id", BasicType::INT32);
    clazz = "stream.nebula.IntegerMapFunction";
    inputClass = "java.lang.Integer";
    outputClass = "java.lang.Integer";

    int32_t initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int32>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with short objects as input and output (IntegerMapFunction<Short, Short>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ShortUDFTest) {
    input = Schema::create()->addField("id", BasicType::INT16);
    output = Schema::create()->addField("id", BasicType::INT16);
    clazz = "stream.nebula.ShortMapFunction";
    inputClass = "java.lang.Short";
    outputClass = "java.lang.Short";

    int16_t initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int16>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with byte objects as input and output (IntegerMapFunction<Byte, Byte>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ByteUDFTest) {
    input = Schema::create()->addField("id", BasicType::INT8);
    output = Schema::create()->addField("id", BasicType::INT8);
    clazz = "stream.nebula.ByteMapFunction";
    inputClass = "java.lang.Byte";
    outputClass = "java.lang.Byte";

    int8_t initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int8>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with long objects as input and output (IntegerMapFunction<Long, Long>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, LongUDFTest) {
    input = Schema::create()->addField("id", BasicType::INT64);
    output = Schema::create()->addField("id", BasicType::INT64);
    clazz = "stream.nebula.LongMapFunction";
    inputClass = "java.lang.Long";
    outputClass = "java.lang.Long";

    int64_t initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int64>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with long objects as input and output (IntegerMapFunction<Long, Long>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, UnsignedLongUDFTest) {
    input = Schema::create()->addField("id", BasicType::UINT64);
    output = Schema::create()->addField("id", BasicType::INT64);
    clazz = "stream.nebula.LongMapFunction";
    inputClass = "java.lang.Long";
    outputClass = "java.lang.Long";

    int64_t initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int64>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10);
}

/**
 * @brief Test simple UDF with double objects as input and output (IntegerMapFunction<Double, Double>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, DoubleUDFTest) {
    input = Schema::create()->addField("id", BasicType::FLOAT64);
    output = Schema::create()->addField("id", BasicType::FLOAT64);
    clazz = "stream.nebula.DoubleMapFunction";
    inputClass = "java.lang.Double";
    outputClass = "java.lang.Double";

    double initialValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Double>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10.0);
}

/**
 * @brief Test simple UDF with float objects as input and output (FloatMapFunction<Float, Float>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, FloatUDFTest) {
    input = Schema::create()->addField("id", BasicType::FLOAT32);
    output = Schema::create()->addField("id", BasicType::FLOAT32);
    clazz = "stream.nebula.FloatMapFunction";
    inputClass = "java.lang.Float";
    outputClass = "java.lang.Float";

    float initialValue = 42.0;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Float>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initialValue + 10.0);
}

/**
 * @brief Test simple UDF with boolean objects as input and output (BooleanMapFunction<Boolean, Boolean>)
 * The UDF sets incoming tuples to false.
*/
TEST_F(MapJavaUdfOperatorTest, BooleanUDFTest) {
    input = Schema::create()->addField("id", BasicType::BOOLEAN);
    output = Schema::create()->addField("id", BasicType::BOOLEAN);
    clazz = "stream.nebula.BooleanMapFunction";
    inputClass = "java.lang.Boolean";
    outputClass = "java.lang.Boolean";

    auto initialValue = true;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Boolean>(initialValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), false);
}

/**
 * @brief Test simple UDF with string objects as input and output (StringMapFunction<String, String>)
 * The UDF appends incoming tuples the postfix 'appended'.
 * //TODO After fixing the text equal function this test fails, the bug is specified in issue #3625
*/
TEST_F(MapJavaUdfOperatorTest, DISABLED_StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    input = Schema::create()->addField("id", BasicType::TEXT);
    output = Schema::create()->addField("id", BasicType::TEXT);
    clazz = "stream.nebula.StringMapFunction";
    inputClass = "java.lang.String";
    outputClass = "java.lang.String";

    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) &wc), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Text>("testValue")}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
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
    input = Schema::create()
                ->addField("byteVariable", BasicType::INT8)
                ->addField("shortVariable", BasicType::INT16)
                ->addField("intVariable", BasicType::INT32)
                ->addField("longVariable", BasicType::INT64)
                ->addField("unsignedLongVariable", BasicType::UINT64) // UINT64 input fields are also mapped to Java long
                ->addField("floatVariable", BasicType::FLOAT32)
                ->addField("doubleVariable", BasicType::FLOAT64)
                ->addField("stringVariable", BasicType::TEXT)
                ->addField("booleanVariable", BasicType::BOOLEAN);
    output = Schema::create()
                 ->addField("byteVariable", BasicType::INT8)
                 ->addField("shortVariable", BasicType::INT16)
                 ->addField("intVariable", BasicType::INT32)
                 ->addField("longVariable", BasicType::INT64)
                 ->addField("unsignedLongVariable", BasicType::INT64) // Java long are always mapped to INT64 in output
                 ->addField("floatVariable", BasicType::FLOAT32)
                 ->addField("doubleVariable", BasicType::FLOAT64)
                 ->addField("stringVariable", BasicType::TEXT)
                 ->addField("booleanVariable", BasicType::BOOLEAN);
    clazz = "stream.nebula.ComplexPojoMapFunction";
    inputClass = "stream.nebula.ComplexPojo";
    outputClass = "stream.nebula.ComplexPojo";

    int8_t initialByte = 10;
    int16_t initialShort = 10;
    int32_t initialInt = 10;
    int64_t initialLong = 10;
    uint64_t initialUnsignedLong = 10;
    float initialFloat = 10.0;
    double initialDouble = 10.0;
    bool initialBool = true;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"byteVariable", Value<Int8>(initialByte)},
                          {"shortVariable", Value<Int16>(initialShort)},
                          {"intVariable", Value<Int32>(initialInt)},
                          {"longVariable", Value<Int64>(initialLong)},
                          {"unsignedLongVariable", Value<UInt64>(initialUnsignedLong)},
                          {"floatVariable", Value<Float>(initialFloat)},
                          {"doubleVariable", Value<Double>(initialDouble)},
                          {"stringVariable", Value<Text>("testValue")},
                          {"booleanVariable", Value<Boolean>(initialBool)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    EXPECT_EQ(result.read("byteVariable"), initialByte + 10);
    EXPECT_EQ(result.read("shortVariable"), initialShort + 10);
    EXPECT_EQ(result.read("intVariable"), initialInt + 10);
    EXPECT_EQ(result.read("longVariable"), initialLong + 10);
    EXPECT_EQ(result.read("longVariable"), initialUnsignedLong + 10);
    EXPECT_EQ(result.read("floatVariable"), initialFloat + 10.0);
    EXPECT_EQ(result.read("doubleVariable"), initialDouble + 10.0);
    // EXPECT_EQ(record.read("stringVariable"), Value<Text>("testValue_appended"));
    //TODO This is also affected by issue #3625, as the map function is not producing the expected output
    EXPECT_EQ(result.read("booleanVariable"), false);
}

/**
* @brief Test UDF with multiple internal dependencies (DummyRichMapFunction<Integer, Integer>)
*/
TEST_F(MapJavaUdfOperatorTest, DependenciesUDFTest) {
    input = Schema::create()->addField("id", BasicType::INT32);
    output = Schema::create()->addField("id", BasicType::INT32);
    clazz = "stream.nebula.DummyRichMapFunction";
    inputClass = "java.lang.Integer";
    outputClass = "java.lang.Integer";

    auto initalValue = 42;
    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = MapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Int32>(initalValue)}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records[0];
    ASSERT_EQ(result.read("id"), initalValue + 10);
}

}// namespace NES::Runtime::Execution::Operators