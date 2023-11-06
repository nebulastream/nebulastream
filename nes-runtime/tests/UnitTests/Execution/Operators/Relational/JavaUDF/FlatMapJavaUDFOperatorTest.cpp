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
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>

using namespace std::string_literals;

namespace NES::Runtime::Execution::Operators {

class FlatMapJavaUDFOperatorTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("FlatMapJavaUDFOperatorTest.log", NES::LogLevel::LOG_DEBUG); }
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
    auto setupAndExecuteFlatMapUdf(const std::string& className,
                                   const std::string& inputClass,
                                   jni::JavaUDFByteCodeList byteCodeList,
                                   const SchemaPtr& inputSchema,
                                   const SchemaPtr& outputSchema,
                                   int8_t* buffer,
                                   Record record) {
        loadByteCode(byteCodeList);
        const auto method = "flatMap"s;
        const jni::JavaSerializedInstance instance;
        const std::optional<std::string> classPath = std::nullopt;
        auto handler = std::make_shared<JavaUDFOperatorHandler>(className,
                                                                method,
                                                                inputClass,
                                                                "java.util.Collection",
                                                                byteCodeList,
                                                                instance,
                                                                inputSchema,
                                                                outputSchema,
                                                                classPath);
        auto map = FlatMapJavaUDF(0, inputSchema, outputSchema);
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
    auto outputRecords =
        setupAndExecuteFlatMapUdf("stream.nebula.StringFlatMapFunction",
                                  "java.lang.String",
                                  {{"stream.nebula.FlatMapFunction", {}}, {{"stream.nebula.StringFlatMapFunction"}, {}}},
                                  Schema::create()->addField("id", BasicType::TEXT),
                                  Schema::create()->addField("id", BasicType::TEXT),
                                  (int8_t*) &wc,
                                  inputRecord);
    ASSERT_EQ(outputRecords.size(), 2);
    ASSERT_EQ(outputRecords[0].read("id"), Value<Text>("Hallo"));
    ASSERT_EQ(outputRecords[1].read("id"), Value<Text>("World"));
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
    auto outputRecords = setupAndExecuteFlatMapUdf("stream.nebula.ComplexPojoFlatMapFunction",
                                                   "stream.nebula.ComplexPojo",
                                                   {{"stream.nebula.FlatMapFunction", {}},
                                                    {"stream.nebula.ComplexPojoFlatMapFunction", {}},
                                                    {"stream.nebula.ComplexPojo", {}}},
                                                   schema,
                                                   schema,
                                                   (int8_t*) &wc,
                                                   inputRecord);
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