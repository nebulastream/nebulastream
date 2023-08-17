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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/FlatMapJavaUDF.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class FlatMapJavaUDFOperatorTest : public Testing::TestWithErrorHandling {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FlatMapJavaUDFOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup FlatMapJavaUDFOperatorTest test class." << std::endl;
    }
};

const std::string method = "flatMap";
jni::JavaUDFByteCodeList byteCodeList;
jni::JavaSerializedInstance serializedInstance;
SchemaPtr input, output;
std::string clazz, inputClass, outputClass;

/**
 * @brief Test flatmap UDF with string objects as input and output (StringFlatMapFunction<String, String>)
 * The UDF splits the input string and returns a list of all words.
*/
TEST_F(FlatMapJavaUDFOperatorTest, StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    input = Schema::create()->addField("id", BasicType::TEXT);
    output = Schema::create()->addField("id", BasicType::TEXT);
    clazz = "stream.nebula.StringFlatMapFunction";
    inputClass = "java.lang.String";
    outputClass = "java.util.Collection";

    auto handler = std::make_shared<JavaUDFOperatorHandler>(clazz,
                                                            method,
                                                            inputClass,
                                                            outputClass,
                                                            byteCodeList,
                                                            serializedInstance,
                                                            input,
                                                            output,
                                                            std::nullopt);
    auto map = FlatMapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) &wc), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"id", Value<Text>("Hallo World")}});
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    map.setup(ctx);
    map.open(ctx, recordBuffer);
    map.execute(ctx, record);
    auto result = collector->records;
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result[0].read("id"), Value<Text>("Hallo"));
    ASSERT_EQ(result[1].read("id"), Value<Text>("World"));
}

/**
 * @brief Test UDF with loaded java classes as input and output (ComplexFlatMapFunction<ComplexPojo, ComplexPojo>)
 * The UDF sets the bool to false, numerics +10 and appends to strings the postfix 'appended'.
*/
TEST_F(FlatMapJavaUDFOperatorTest, ComplexPojoFlatMapFunction) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    input = Schema::create()
                ->addField("byteVariable", BasicType::INT8)
                ->addField("shortVariable", BasicType::INT16)
                ->addField("intVariable", BasicType::INT32)
                ->addField("longVariable", BasicType::INT64)
                ->addField("floatVariable", BasicType::FLOAT32)
                ->addField("doubleVariable", BasicType::FLOAT64)
                ->addField("stringVariable", BasicType::TEXT)
                ->addField("booleanVariable", BasicType::BOOLEAN);
    output = Schema::create()
                 ->addField("byteVariable", BasicType::INT8)
                 ->addField("shortVariable", BasicType::INT16)
                 ->addField("intVariable", BasicType::INT32)
                 ->addField("longVariable", BasicType::INT64)
                 ->addField("floatVariable", BasicType::FLOAT32)
                 ->addField("doubleVariable", BasicType::FLOAT64)
                 ->addField("stringVariable", BasicType::TEXT)
                 ->addField("booleanVariable", BasicType::BOOLEAN);
    clazz = "stream.nebula.ComplexPojoFlatMapFunction";
    inputClass = "stream.nebula.ComplexPojo";
    outputClass = "java.util.Collection";

    int8_t initialByte = 10;
    int16_t initialShort = 10;
    int32_t initialInt = 10;
    int64_t initialLong = 10;
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
    auto map = FlatMapJavaUDF(0, input, output);
    auto collector = std::make_shared<CollectOperator>();
    map.setChild(collector);
    auto pipelineContext = MockedPipelineExecutionContext({handler});
    auto ctx = ExecutionContext(Value<MemRef>(nullptr), Value<MemRef>((int8_t*) &pipelineContext));
    auto record = Record({{"byteVariable", Value<Int8>(initialByte)},
                          {"shortVariable", Value<Int16>(initialShort)},
                          {"intVariable", Value<Int32>(initialInt)},
                          {"longVariable", Value<Int64>(initialLong)},
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
    EXPECT_EQ(result.read("floatVariable"), initialFloat + 10.0);
    EXPECT_EQ(result.read("doubleVariable"), initialDouble + 10.0);
    EXPECT_EQ(result.read("stringVariable"), Value<Text>("Appended String:testValue"));
    EXPECT_EQ(result.read("booleanVariable"), false);
}

}// namespace NES::Runtime::Execution::Operators