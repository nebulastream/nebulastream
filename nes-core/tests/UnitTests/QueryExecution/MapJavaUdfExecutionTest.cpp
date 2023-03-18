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

#include <API/Schema.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestExecutionEngine.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <jni.h>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

class MapJavaUdfQueryExecutionTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MaQueryCompilerpJavaUdfQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("QueryExecutionTest: Setup MapJavaUdfQueryExecutionTest test class.");
    }
    /* Will be called before a test is executed. */
    void SetUp() override {
        testing::Test::SetUp();
        executionEngine = std::make_shared<TestExecutionEngine>(QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER);
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        testing::Test::TearDown();
        NES_DEBUG("QueryExecutionTest: Tear down MapJavaUdfQueryExecutionTest test case.");
        ASSERT_TRUE(executionEngine->stop());
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("MapJavaUdfQueryExecutionTest: Tear down QueryExecutionTest test class."); }

    std::shared_ptr<TestExecutionEngine> executionEngine;
    std::string testDataPath = std::string(TEST_DATA_DIRECTORY) + "/JavaUdfTestData/";
};

/**
 * This helper function fills a buffer with test data
 */
void fillBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& buf) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        buf[recordIndex][0].write<int32_t>(recordIndex);
    }
    buf.setNumberOfTuples(10);
}

/**
 * This helper function creates a JVM and returns the JNIEnv.
 */
void createJVM(JavaVM* jvm, JNIEnv** env) {
    JavaVMInitArgs args {};
    std::vector<std::string> opt {"-verbose:jni", "-verbose:class"};
    std::vector<JavaVMOption> options;
    for (const auto& s : opt) {
        options.push_back(JavaVMOption{.optionString = const_cast<char*>(s.c_str())});
    }
    args.version = JNI_VERSION_1_2;
    args.ignoreUnrecognized = false;
    args.options = options.data();
    args.nOptions = std::size(options);
    JNI_CreateJavaVM(&jvm, (void**) env, &args);
}

/**
 * This helper function loads the class files into a buffer.
 */
std::vector<char> loadClassFileIntoBuffer(const std::string& path, const std::string& className) {
    // Open the file
    std::ifstream file(path + className + ".class", std::ios::binary);
    if (!file.is_open()) {
        return {};
    }

    // Get the file size and allocate a buffer
    file.seekg(0, std::ios::end);
    std::streamsize fileSize = file.tellg();
    std::vector<char> buffer(fileSize);

    // Read the file into the buffer
    file.seekg(0, std::ios::beg);
    if (!file.read(buffer.data(), fileSize)) {
        return {};
    }

    // Close the file and return the buffer
    file.close();
    return buffer;
}

/**
 * @brief Test simple UDF with integer objects as input and output (IntegerMapFunction<Integer, Integer>)
 * The UDF increments incoming tuples by 10.
*/
TEST_F(MapJavaUdfQueryExecutionTest, MapJavaUdf) {
    auto schema = Schema::create()->addField("test$id", BasicType::INT32);
    auto testSink = executionEngine->createDataSink(schema);
    auto testSourceDescriptor = executionEngine->createDataSource(schema);

    std::vector<std::string> classNames = {"IntegerMapFunction", "MapFunction"};
    auto methodName = "map";
    std::vector<char> serializedInstance = {};
    auto byteCodeList = std::unordered_map<std::string, std::vector<char>>();
    for (auto &className : classNames) {
        auto buffer = loadClassFileIntoBuffer(testDataPath, className);
        byteCodeList.insert(std::make_pair(className, buffer));
    }

    auto className = classNames[0];
    auto outputSchema = Schema::create()->addField("test$id", BasicType::INT32);
    auto inputClassName = "java/lang/Integer";
    auto outputClassName = "java/lang/Integer";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptor::create(className,
                                                                        methodName,
                                                                        serializedInstance,
                                                                        byteCodeList,
                                                                        outputSchema,
                                                                        inputClassName,
                                                                        outputClassName);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);
    auto query = TestQuery::from(testSourceDescriptor).mapJavaUdf(javaUdfDescriptor).sink(testSinkDescriptor);
    auto plan = executionEngine->submitQuery(query.getQueryPlan());
    auto source = executionEngine->getDataSource(plan, 0);
    ASSERT_TRUE(!!source);
    auto inputBuffer = executionEngine->getBuffer(schema);
    fillBuffer(inputBuffer);
    source->emitBuffer(inputBuffer);
    testSink->waitTillCompleted();
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);
    auto resultBuffer = testSink->getResultBuffer(0);

    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 10u);
    for (uint32_t recordIndex = 0u; recordIndex < 10u; ++recordIndex) {
        EXPECT_EQ(resultBuffer[recordIndex][0].read<int32_t>(), recordIndex + 10);
    }
    ASSERT_TRUE(executionEngine->stopQuery(plan));
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

#endif // ENABLE_JNI