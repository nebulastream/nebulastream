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
#include <BaseUnitTest.hpp>
#include <Execution/Expressions/ArithmeticalExpressions/AddExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

using namespace std::string_literals;

namespace NES::Runtime::Execution::Operators {
class MapJavaUdfOperatorTest : public NES::Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("MapJavaUdfOperatorTest.log", NES::LogLevel::LOG_DEBUG); }
    /** Takes an byte code list that contains class names but no class definitions and loads the byte code for the classes from the test data directory. */
    void loadByteCode(jni::JavaUDFByteCodeList& byteCodeList) {
        for (auto& [className, byteCode] : byteCodeList) {
            const auto fileName = std::filesystem::path(TEST_DATA_DIRECTORY) / "JavaUDFTestData"
                / fmt::format("{}.class", JavaUDFOperatorHandler::convertToJNIName(className));
            NES_DEBUG("Loading byte code: class={}, file={}", className, fileName.string());
            std::ifstream classFile(fileName, std::fstream::binary);
            NES_ASSERT(classFile, "Could not find class file: " << fileName);
            classFile.seekg(0, std::ios_base::end);
            auto fileSize = classFile.tellg();
            classFile.seekg(0, std::ios_base::beg);
            byteCode.resize(fileSize);
            classFile.read(reinterpret_cast<char*>(byteCode.data()), byteCode.size());
        }
    }
    // It would be nice to pass the parameters as a JavaUDFDescriptor
    auto setupAndExecuteMapUdfWithBuffer(const std::string& className,
                                         const std::string& inputClass,
                                         const std::string& outputClass,
                                         jni::JavaUDFByteCodeList byteCodeList,
                                         const SchemaPtr& inputSchema,
                                         const SchemaPtr& outputSchema,
                                         int8_t* buffer,
                                         Record record) {
        loadByteCode(byteCodeList);
        const auto method = "map"s;
        const jni::JavaSerializedInstance instance;
        const std::optional<std::string> classPath = std::nullopt;
        auto handler = std::make_shared<JavaUDFOperatorHandler>(className,
                                                                method,
                                                                inputClass,
                                                                outputClass,
                                                                byteCodeList,
                                                                instance,
                                                                inputSchema,
                                                                outputSchema,
                                                                classPath);
        auto map = MapJavaUDF(0, inputSchema, outputSchema);
        auto collector = std::make_shared<CollectOperator>();
        map.setChild(collector);
        auto pipelineContext = MockedPipelineExecutionContext({handler});
        auto ctx = ExecutionContext(Value<MemRef>(buffer), Value<MemRef>((int8_t*) &pipelineContext));
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
        map.setup(ctx);
        map.open(ctx, recordBuffer);
        map.execute(ctx, record);
        return collector->records[0];
    }
    auto setupAndExecuteMapUdf(const std::string& className,
                               const std::string& inputClass,
                               const std::string& outputClass,
                               jni::JavaUDFByteCodeList byteCodeList,
                               const SchemaPtr& inputSchema,
                               const SchemaPtr& outputSchema,
                               Record record) {
        return setupAndExecuteMapUdfWithBuffer(className,
                                               inputClass,
                                               outputClass,
                                               byteCodeList,
                                               inputSchema,
                                               outputSchema,
                                               nullptr,
                                               record);
    }
};

/**
* @brief Test simple UDF with byte objects as input and output (IntegerMapFunction<Byte, Byte>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ByteUDFTest) {
    int8_t initialValue = 8;
    auto inputRecord = Record({{"id", Value<Int8>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.ByteMapFunction"s,
                                              "java.lang.Byte"s,
                                              "java.lang.Byte"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.ByteMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::INT8),
                                              Schema::create()->addField("id", BasicType::INT8),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}
/**
* @brief Test simple UDF with short objects as input and output (IntegerMapFunction<Short, Short>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, ShortUDFTest) {
    int16_t initialValue = 16;
    auto inputRecord = Record({{"id", Value<Int16>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.ShortMapFunction"s,
                                              "java.lang.Short"s,
                                              "java.lang.Short"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.ShortMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::INT16),
                                              Schema::create()->addField("id", BasicType::INT16),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, IntegerUDFTest) {
    int32_t initialValue = 32;
    auto inputRecord = Record({{"id", Value<Int32>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.IntegerMapFunction"s,
                                              "java.lang.Integer"s,
                                              "java.lang.Integer"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.IntegerMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::INT32),
                                              Schema::create()->addField("id", BasicType::INT32),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with long objects as input and output (IntegerMapFunction<Long, Long>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, LongUDFTest) {
    int64_t initialValue = -64;
    auto inputRecord = Record({{"id", Value<Int64>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.LongMapFunction"s,
                                              "java.lang.Long"s,
                                              "java.lang.Long"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.LongMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::INT64),
                                              Schema::create()->addField("id", BasicType::INT64),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with long objects as input and output (IntegerMapFunction<Long, Long>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, UnsignedLongUDFTest) {
    uint64_t initialValue = 64;
    auto inputRecord = Record({{"id", Value<UInt64>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.LongMapFunction"s,
                                              "java.lang.Long"s,
                                              "java.lang.Long"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.LongMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::UINT64),
                                              Schema::create()->addField("id", BasicType::INT64),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}

/**
* @brief Test simple UDF with float objects as input and output (FloatMapFunction<Float, Float>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, FloatUDFTest) {
    float initialValue = 23.0;
    auto inputRecord = Record({{"id", Value<Float>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.FloatMapFunction"s,
                                              "java.lang.Float"s,
                                              "java.lang.Float"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.FloatMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::FLOAT32),
                                              Schema::create()->addField("id", BasicType::FLOAT32),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10.0);
}

/**
* @brief Test simple UDF with double objects as input and output (IntegerMapFunction<Double, Double>)
* The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfOperatorTest, DoubleUDFTest) {
    double initialValue = 42.0;
    auto inputRecord = Record({{"id", Value<Double>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.DoubleMapFunction"s,
                                              "java.lang.Double"s,
                                              "java.lang.Double"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.DoubleMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::FLOAT64),
                                              Schema::create()->addField("id", BasicType::FLOAT64),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10.0);
}

/**
* @brief Test simple UDF with boolean objects as input and output (BooleanMapFunction<Boolean, Boolean>)
* The UDF sets incoming tuples to false.
*/
TEST_F(MapJavaUdfOperatorTest, BooleanUDFTest) {
    auto initialValue = true;
    auto inputRecord = Record({{"id", Value<Boolean>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.BooleanMapFunction"s,
                                              "java.lang.Boolean"s,
                                              "java.lang.Boolean"s,
                                              {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.BooleanMapFunction"s}, {}}},
                                              Schema::create()->addField("id", BasicType::BOOLEAN),
                                              Schema::create()->addField("id", BasicType::BOOLEAN),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), false);
}

/**
* @brief Test simple UDF with string objects as input and output (StringMapFunction<String, String>)
* The UDF prepends the prefix 'Appended String:'.
*/
TEST_F(MapJavaUdfOperatorTest, StringUDFTest) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    auto inputRecord = Record({{"id", Value<Text>("testValue")}});
    auto outputRecord =
        setupAndExecuteMapUdfWithBuffer("stream.nebula.StringMapFunction"s,
                                        "java.lang.String"s,
                                        "java.lang.String"s,
                                        {{{"stream.nebula.MapFunction"}, {}}, {{"stream.nebula.StringMapFunction"s}, {}}},
                                        Schema::create()->addField("id", BasicType::TEXT),
                                        Schema::create()->addField("id", BasicType::TEXT),
                                        (int8_t*) &wc,
                                        inputRecord);
    ASSERT_EQ(outputRecord.read("id"), Value<Text>("Appended String:testValue"));
}

/**
* @brief Test simple UDF with loaded java classes as input and output (ComplexMapFunction<ComplexPojo, ComplexPojo>)
* The UDF sets the bool to false, numerics +10 and prepends the prefix 'Appended String:'.
*/
TEST_F(MapJavaUdfOperatorTest, ComplexPojoMapFunction) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<Runtime::WorkerContext>(-1, bm, 1024);
    auto inputSchema =
        Schema::create()
            ->addField("byteVariable", BasicType::INT8)
            ->addField("shortVariable", BasicType::INT16)
            ->addField("intVariable", BasicType::INT32)
            ->addField("longVariable", BasicType::INT64)
            ->addField("unsignedLongVariable", BasicType::UINT64)// UINT64 input fields are also mapped to Java long
            ->addField("floatVariable", BasicType::FLOAT32)
            ->addField("doubleVariable", BasicType::FLOAT64)
            ->addField("stringVariable", BasicType::TEXT)
            ->addField("booleanVariable", BasicType::BOOLEAN);
    auto outputSchema = Schema::create()
                            ->addField("byteVariable", BasicType::INT8)
                            ->addField("shortVariable", BasicType::INT16)
                            ->addField("intVariable", BasicType::INT32)
                            ->addField("longVariable", BasicType::INT64)
                            ->addField("unsignedLongVariable", BasicType::INT64)// Java long are always mapped to INT64 in output
                            ->addField("floatVariable", BasicType::FLOAT32)
                            ->addField("doubleVariable", BasicType::FLOAT64)
                            ->addField("stringVariable", BasicType::TEXT)
                            ->addField("booleanVariable", BasicType::BOOLEAN);
    int8_t initialByte = 10;
    int16_t initialShort = 10;
    int32_t initialInt = 10;
    int64_t initialLong = 10;
    uint64_t initialUnsignedLong = 10;
    float initialFloat = 10.0;
    double initialDouble = 10.0;
    bool initialBool = true;
    auto inputRecord = Record({{"byteVariable", Value<Int8>(initialByte)},
                               {"shortVariable", Value<Int16>(initialShort)},
                               {"intVariable", Value<Int32>(initialInt)},
                               {"longVariable", Value<Int64>(initialLong)},
                               {"unsignedLongVariable", Value<UInt64>(initialUnsignedLong)},
                               {"floatVariable", Value<Float>(initialFloat)},
                               {"doubleVariable", Value<Double>(initialDouble)},
                               {"stringVariable", Value<Text>("testValue")},
                               {"booleanVariable", Value<Boolean>(initialBool)}});
    auto outputRecord = setupAndExecuteMapUdfWithBuffer("stream.nebula.ComplexPojoMapFunction"s,
                                                        "stream.nebula.ComplexPojo"s,
                                                        "stream.nebula.ComplexPojo"s,
                                                        {{{"stream.nebula.MapFunction"}, {}},
                                                         {{"stream.nebula.ComplexPojoMapFunction"}, {}},
                                                         {{"stream.nebula.ComplexPojo"}, {}}},
                                                        inputSchema,
                                                        outputSchema,
                                                        (int8_t*) &wc,
                                                        inputRecord);
    EXPECT_EQ(outputRecord.read("byteVariable"), initialByte + 10);
    EXPECT_EQ(outputRecord.read("shortVariable"), initialShort + 10);
    EXPECT_EQ(outputRecord.read("intVariable"), initialInt + 10);
    EXPECT_EQ(outputRecord.read("longVariable"), initialLong + 10);
    EXPECT_EQ(outputRecord.read("longVariable"), initialUnsignedLong + 10);
    EXPECT_EQ(outputRecord.read("floatVariable"), initialFloat + 10.0);
    EXPECT_EQ(outputRecord.read("doubleVariable"), initialDouble + 10.0);
    EXPECT_EQ(outputRecord.read("stringVariable"), Value<Text>("testValueAppended String:"));
    EXPECT_EQ(outputRecord.read("booleanVariable"), false);
}

/**
* @brief Test UDF with multiple internal dependencies (DummyRichMapFunction<Integer, Integer>)
*/
TEST_F(MapJavaUdfOperatorTest, DependenciesUDFTest) {
    auto initialValue = 42;
    auto inputRecord = Record({{"id", Value<Int32>(initialValue)}});
    auto outputRecord = setupAndExecuteMapUdf("stream.nebula.DummyRichMapFunction"s,
                                              "java.lang.Integer"s,
                                              "java.lang.Integer"s,
                                              {{{"stream.nebula.MapFunction"}, {}},
                                               {{"stream.nebula.DummyRichMapFunction"}, {}},
                                               {{"stream.nebula.DummyRichMapFunction$DependentClass"}, {}},
                                               {{"stream.nebula.DummyRichMapFunction$RecursiveDependentClass"}, {}}},
                                              Schema::create()->addField("id", BasicType::INT32),
                                              Schema::create()->addField("id", BasicType::INT32),
                                              inputRecord);
    ASSERT_EQ(outputRecord.read("id"), initialValue + 10);
}

}// namespace NES::Runtime::Execution::Operators
