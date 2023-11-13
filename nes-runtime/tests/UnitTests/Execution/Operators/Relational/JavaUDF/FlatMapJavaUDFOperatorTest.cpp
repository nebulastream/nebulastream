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
#include <BaseIntegrationTest.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/FlatMapJavaUDF.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>

using namespace std::string_literals;

namespace NES::Runtime::Execution::Operators {

class FlatMapJavaUDFOperatorTest : public Testing::BaseUnitTest {
public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("FlatMapJavaUDFOperatorTest.log", NES::LogLevel::LOG_DEBUG); }

    auto setupAndExecuteFlatMapUdf(Catalogs::UDF::JavaUDFDescriptorPtr javaUDFDescriptor, int8_t* buffer, Record record) {
        const auto method = "flatMap"s;
        const jni::JavaSerializedInstance instance;
        const std::optional<std::string> classPath = std::nullopt;
        auto handler = std::make_shared<JavaUDFOperatorHandler>(javaUDFDescriptor->getClassName(),
                                                                method,
                                                                javaUDFDescriptor->getInputClassName(),
                                                                "java.util.Collection",
                                                                javaUDFDescriptor->getByteCodeList(),
                                                                instance,
                                                                javaUDFDescriptor->getInputSchema(),
                                                                javaUDFDescriptor->getOutputSchema(),
                                                                classPath);
        auto map = FlatMapJavaUDF(0, javaUDFDescriptor->getInputSchema(), javaUDFDescriptor->getOutputSchema());
        auto collector = std::make_shared<CollectOperator>();
        map.setChild(collector);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        auto ctx = ExecutionContext(Value<MemRef>(buffer), Value<MemRef>((int8_t*) &pipelineContext));
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
        map.setup(ctx);
        map.open(ctx, recordBuffer);
        map.execute(ctx, record);
        return collector->records;
    }
};

/**
 * @brief Test flatmap UDF with string objects as input and output (StringFlatMapFunction<String, String>)
 * The UDF splits the input string and returns a list of all words.
*/
TEST_F(FlatMapJavaUDFOperatorTest, StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    auto inputRecord = Record({{"id", Value<Text>("Hallo World")}});
    auto javaUDFDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder()
                             .setClassName("stream.nebula.StringFlatMapFunction")
                             .setInputClassName("java.lang.String")
                             .setByteCodeList({{"stream.nebula.FlatMapFunction", {}},
                                               {{"stream.nebula.StringFlatMapFunction"}, {}}})
                             .setInputSchema(Schema::create()->addField("id", BasicType::TEXT))
                             .setOutputSchema(Schema::create()->addField("id", BasicType::TEXT))
                             .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                             .build();
    auto outputRecords = setupAndExecuteFlatMapUdf(javaUDFDescriptor, (int8_t*) &wc, inputRecord);
    ASSERT_EQ(outputRecords.size(), 2);
    EXPECT_EQ(outputRecords[0].read("id"), Value<Text>("Hallo"));
    EXPECT_EQ(outputRecords[1].read("id"), Value<Text>("World"));
}

/**
 * @brief Test UDF with loaded java classes as input and output (ComplexFlatMapFunction<ComplexPojo, ComplexPojo>)
 * The UDF sets the bool to false, numerics +10 and appends to strings the postfix 'appended'.
*/
TEST_F(FlatMapJavaUDFOperatorTest, ComplexPojoFlatMapFunction) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    auto schema = Schema::create()
                  ->addField("byteVariable", BasicType::INT8)
                  ->addField("shortVariable", BasicType::INT16)
                  ->addField("intVariable", BasicType::INT32)
                  ->addField("longVariable", BasicType::INT64)
                  ->addField("floatVariable", BasicType::FLOAT32)
                  ->addField("doubleVariable", BasicType::FLOAT64)
                  ->addField("stringVariable", BasicType::TEXT)
                  ->addField("booleanVariable", BasicType::BOOLEAN);
    int8_t initialByte = 10;
    int16_t initialShort = 10;
    int32_t initialInt = 10;
    int64_t initialLong = 10;
    float initialFloat = 10.0;
    double initialDouble = 10.0;
    bool initialBool = true;
    auto inputRecord = Record({{"byteVariable", Value<Int8>(initialByte)},
                               {"shortVariable", Value<Int16>(initialShort)},
                               {"intVariable", Value<Int32>(initialInt)},
                               {"longVariable", Value<Int64>(initialLong)},
                               {"floatVariable", Value<Float>(initialFloat)},
                               {"doubleVariable", Value<Double>(initialDouble)},
                               {"stringVariable", Value<Text>("testValue")},
                               {"booleanVariable", Value<Boolean>(initialBool)}});
    auto javaUDFDescriptor = Catalogs::UDF::JavaUDFDescriptorBuilder()
                             .setClassName("stream.nebula.ComplexPojoFlatMapFunction")
                             .setInputClassName("stream.nebula.ComplexPojo")
                             .setByteCodeList({{"stream.nebula.FlatMapFunction", {}},
                                               {"stream.nebula.ComplexPojoFlatMapFunction", {}},
                                               {"stream.nebula.ComplexPojo", {}}})
                             .setInputSchema(schema)
                             .setOutputSchema(schema)
                             .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                             .build();
    auto outputRecords = setupAndExecuteFlatMapUdf(javaUDFDescriptor, (int8_t*) &wc, inputRecord);
    auto result = outputRecords[0];
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
