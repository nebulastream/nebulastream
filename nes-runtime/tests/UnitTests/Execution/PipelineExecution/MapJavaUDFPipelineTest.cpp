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
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Relational/MapJavaUdf.hpp>
#include <Execution/Operators/Relational/MapJavaUdfOperatorHandler.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class MapJavaUDFPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapJavaUDFPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup MapJavaUDFPipelineTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup MapJavaUDFPipelineTest test case." << std::endl;
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down MapJavaUDFPipelineTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down MapJavaUDFPipelineTest test class." << std::endl; }

    std::string testDataPath = std::string(TEST_DATA_DIRECTORY) + "/JavaUdfTestData";
};


/**
 * @brief Test a pipeline containing a scan, a java map with integers, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineIntegerMap) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("intVariable", BasicType::INT32);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    std::string className = "IntegerMapFunction";
    std::string methodName = "map";
    std::string inputProxyName = "java/lang/Integer";
    std::string outputProxyName = "java/lang/Integer";
    std::unordered_map<std::string, std::vector<char>> byteCodeList = {};
    std::vector<char> serializedInstance = {};
    SchemaPtr input = Schema::create()->addField("intVariable", NES::INT32);
    SchemaPtr output = Schema::create()->addField("intVariable", NES::INT32);
    auto mapOperator = std::make_shared<Operators::MapJavaUdf>(0, input, output);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        dynamicBuffer[i]["intVariable"].write((int32_t) i);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);
    auto handler = std::make_shared<Operators::MapJavaUdfOperatorHandler>(className, methodName, inputProxyName, outputProxyName, byteCodeList, serializedInstance, input, output, testDataPath);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["intVariable"].read<int32_t>(), i + 10);
    }
}


/**
 * @brief Test a pipeline containing a scan, a java map with booleans, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, DISABLED_scanMapEmitPipelineBooleanMap) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("booleanVariable", BasicType::BOOLEAN);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    std::string className = "BooleanMapFunction";
    std::string methodName = "map";
    std::string inputProxyName = "java/lang/Boolean";
    std::string outputProxyName = "java/lang/Boolean";
    std::unordered_map<std::string, std::vector<char>> byteCodeList = {};
    std::vector<char> serializedInstance = {};
    SchemaPtr input = Schema::create()->addField("booleanVariable", NES::BOOLEAN);
    SchemaPtr output = Schema::create()->addField("booleanVariable", NES::BOOLEAN);
    auto mapOperator = std::make_shared<Operators::MapJavaUdf>(0, input, output);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        dynamicBuffer[i]["booleanVariable"].write((bool) true);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);
    auto handler = std::make_shared<Operators::MapJavaUdfOperatorHandler>(className, methodName, inputProxyName, outputProxyName, byteCodeList, serializedInstance, input, output, testDataPath);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["booleanVariable"].read<bool>(), false);
    }
}

/**
 * @brief Test a pipeline containing a scan, a java map with strings, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineStringMap) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("stringVariable", BasicType::TEXT);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    std::string className = "StringMapFunction";
    std::string methodName = "map";
    std::string inputProxyName = "java/lang/String";
    std::string outputProxyName = "java/lang/String";
    std::unordered_map<std::string, std::vector<char>> byteCodeList = {};
    std::vector<char> serializedInstance = {};
    SchemaPtr input = Schema::create()->addField("stringVariable", NES::TEXT);
    SchemaPtr output = Schema::create()->addField("stringVariable", NES::TEXT);
    auto mapOperator = std::make_shared<Operators::MapJavaUdf>(0, input, output);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        std::string value = "testValue";
        auto varLengthBuffer = bm->getBufferBlocking();
        *varLengthBuffer.getBuffer<uint32_t>() = value.size();
        std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), value.c_str());
        NES_DEBUG("test string: " << (varLengthBuffer.getBuffer<char>() + sizeof(uint32_t)));
        auto index = buffer.storeChildBuffer(varLengthBuffer);
        dynamicBuffer[i]["stringVariable"].write(index);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);
    auto handler = std::make_shared<Operators::MapJavaUdfOperatorHandler>(className, methodName, inputProxyName, outputProxyName, byteCodeList, serializedInstance, input, output, testDataPath);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        auto index = resultDynamicBuffer[i]["stringVariable"].read<uint32_t>();
        auto varLengthBuffer = resultBuffer.loadChildBuffer(index);
        auto textValue = varLengthBuffer.getBuffer<TextValue>();
        auto size = textValue->length();
        char* value = (char *) malloc(size * sizeof(char));
        memcpy(value, varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), size * sizeof(char));
        ASSERT_EQ(std::string(value), "testValue_appended");
    }
}

/**
 * @brief Test a pipeline containing a scan, a java map with multiple types, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, DISABLED_scanMapEmitPipelineComplexMap) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("intVariable", BasicType::INT32);
    schema->addField("floatVariable", BasicType::FLOAT32);
    schema->addField("booleanVariable", BasicType::BOOLEAN);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

    std::string className = "ComplexPojoMapFunction";
    std::string methodName = "map";
    std::string inputProxyName = "ComplexPojo";
    std::string outputProxyName = "ComplexPojo";
    std::unordered_map<std::string, std::vector<char>> byteCodeList = {};
    std::vector<char> serializedInstance = {};
    SchemaPtr input = Schema::create()->addField("intVariable", NES::INT32)->addField("floatVariable", NES::FLOAT32)->addField("booleanVariable", NES::BOOLEAN);
    SchemaPtr output = Schema::create()->addField("intVariable", NES::INT32)->addField("floatVariable", NES::FLOAT32)->addField("booleanVariable", NES::BOOLEAN);
    auto mapOperator = std::make_shared<Operators::MapJavaUdf>(0, input, output);
    scanOperator->setChild(mapOperator);

    auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
    auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
    mapOperator->setChild(emitOperator);

    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    pipeline->setRootOperator(scanOperator);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        dynamicBuffer[i]["intVariable"].write((int32_t) i);
        dynamicBuffer[i]["floatVariable"].write((float) i);
        dynamicBuffer[i]["booleanVariable"].write(true);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline);
    auto handler = std::make_shared<Operators::MapJavaUdfOperatorHandler>(className, methodName, inputProxyName, outputProxyName, byteCodeList, serializedInstance, input, output, testDataPath);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i]["intVariable"].read<int32_t>(), i + 10);
        ASSERT_EQ(resultDynamicBuffer[i]["floatVariable"].read<float>(), i + 10);
        ASSERT_EQ(resultDynamicBuffer[i]["booleanVariable"].read<bool>(), false);
    }
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        MapJavaUDFPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<MapJavaUDFPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution