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
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/Relational/JavaUDF/JavaUDFOperatorHandler.hpp>
#include <Execution/Operators/Relational/JavaUDF/MapJavaUDF.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution {

class MapJavaUDFPipelineTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
    Nautilus::CompilationOptions options;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MapJavaUDFPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MapJavaUDFPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_INFO("Setup MapJavaUDFPipelineTest test case.");
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
        options.setDumpToConsole(true);
    }

    /**
     * Initializes a pipeline with a Scan of the input tuples, a MapJavaUDF operator, and a emit of the processed tuples.
     * @param schema Schema of the input and output tuples.
     * @param memoryLayout memory layout
     * @return
     */
    auto initPipelineOperator(SchemaPtr schema, auto memoryLayout) {
        std::shared_ptr<Operators::ExecutableOperator> mapOperator;

        mapOperator = std::make_shared<Operators::MapJavaUDF>(0, schema, schema);

        auto scanMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto scanOperator = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderPtr));

        scanOperator->setChild(mapOperator);

        auto emitMemoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayout);
        auto emitOperator = std::make_shared<Operators::Emit>(std::move(emitMemoryProviderPtr));
        mapOperator->setChild(emitOperator);

        auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
        pipeline->setRootOperator(scanOperator);
        return pipeline;
    }

    /**
     * Initializes the input buffer for numeric values.
     * @tparam T type of the numeric values
     * @param variableName name of the variable in the schema
     * @param bufferManager buffer manager
     * @param memoryLayout memory layout
     * @return input buffer
     */
    template<typename T>
    auto initInputBuffer(std::string variableName, auto bufferManager, auto memoryLayout) {
        auto buffer = bufferManager->getBufferBlocking();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
        for (uint64_t i = 0; i < 10; i++) {
            dynamicBuffer[i][variableName].write((T) i);
            dynamicBuffer.setNumberOfTuples(i + 1);
        }
        return buffer;
    }

    /**
     * Initializes the map handler for the given pipeline.
     * @param className java class name of the udf
     * @param methodName method name of the udf
     * @param inputProxyName input proxy class name
     * @param outputProxyName output proxy class name
     * @param schema schema of the input and output tuples
     * @param testDataPath path to the test data containing the udf jar
     * @return operator handler
     */
    auto initMapHandler(std::string className,
                        std::string methodName,
                        std::string inputProxyName,
                        std::string outputProxyName,
                        SchemaPtr schema) {
        jni::JavaUDFByteCodeList byteCodeList;
        jni::JavaSerializedInstance serializedInstance;
        return std::make_shared<Operators::JavaUDFOperatorHandler>(className,
                                                                   methodName,
                                                                   inputProxyName,
                                                                   outputProxyName,
                                                                   byteCodeList,
                                                                   serializedInstance,
                                                                   schema,
                                                                   schema,
                                                                   std::nullopt);
    }

    /**
     * Check the output buffer for numeric values.
     * @tparam T type of the numeric values
     * @param variableName name of the variable in the schema
     * @param pipelineContext pipeline context
     * @param memoryLayout memory layout
     */
    template<typename T>
    void checkBufferResult(std::string variableName, auto pipelineContext, auto memoryLayout) {
        ASSERT_EQ(pipelineContext.buffers.size(), 1);
        auto resultBuffer = pipelineContext.buffers[0];
        ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

        auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
        for (uint64_t i = 0; i < 10; i++) {
            ASSERT_EQ(resultDynamicBuffer[i][variableName].read<T>(), i + 10);
        }
    }
};

/**
 * @brief Test a pipeline containing a scan, a java map with integers, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineIntegerMap) {
    auto variableName = "intVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::INT32);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = initInputBuffer<int32_t>(variableName, bm, memoryLayout);
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.IntegerMapFunction", "map", "java.lang.Integer", "java.lang.Integer", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    checkBufferResult<int32_t>(variableName, pipelineContext, memoryLayout);
}

/**
 * @brief Test a pipeline containing a scan, a java map with shorts, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineShortMap) {
    auto variableName = "shortVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::INT16);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = initInputBuffer<int16_t>(variableName, bm, memoryLayout);
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.ShortMapFunction", "map", "java.lang.Short", "java.lang.Short", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    checkBufferResult<int16_t>(variableName, pipelineContext, memoryLayout);
}

/**
 * @brief Test a pipeline containing a scan, a java map with byte, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineByteMap) {
    auto variableName = "byteVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::INT8);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = initInputBuffer<int8_t>(variableName, bm, memoryLayout);
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.ByteMapFunction", "map", "java.lang.Byte", "java.lang.Byte", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    checkBufferResult<int8_t>(variableName, pipelineContext, memoryLayout);
}

/**
 * @brief Test a pipeline containing a scan, a java map with long, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineLongMap) {
    auto variableName = "longVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::INT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = initInputBuffer<int64_t>(variableName, bm, memoryLayout);
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.LongMapFunction", "map", "java.lang.Long", "java.lang.Long", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    checkBufferResult<int64_t>(variableName, pipelineContext, memoryLayout);
}

/**
 * @brief Test a pipeline containing a scan, a java map with shorts, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineDoubleMap) {
    auto variableName = "DoubleVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::FLOAT64);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = initInputBuffer<double>(variableName, bm, memoryLayout);
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.DoubleMapFunction", "map", "java.lang.Double", "java.lang.Double", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    checkBufferResult<double>(variableName, pipelineContext, memoryLayout);
}

/**
 * @brief Test a pipeline containing a scan, a java map with booleans, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineBooleanMap) {
    auto variableName = "BooleanVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::BOOLEAN);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);
    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        dynamicBuffer[i][variableName].write((bool) true);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }
    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.BooleanMapFunction", "map", "java.lang.Boolean", "java.lang.Boolean", schema);
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resultDynamicBuffer[i][variableName].read<bool>(), false);
    }
}

/**
 * @brief Test a pipeline containing a scan, a java map with strings, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineStringMap) {
    auto variableName = "stringVariable";
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)->addField(variableName, BasicType::TEXT);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        std::string value = "X";
        auto varLengthBuffer = bm->getBufferBlocking();
        *varLengthBuffer.getBuffer<uint32_t>() = value.size();
        std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), value.c_str());
        auto index = buffer.storeChildBuffer(varLengthBuffer);
        dynamicBuffer[i]["stringVariable"].write(index);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.StringMapFunction", "map", "java.lang.String", "java.lang.String", schema);

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
        ASSERT_EQ(std::string(textValue->c_str(), size), "Appended String:X");
    }
}

/**
 * @brief Test a pipeline containing a scan, a java map with multiple types, and a emit operator
 */
TEST_P(MapJavaUDFPipelineTest, scanMapEmitPipelineComplexMap) {
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("stringVariable", BasicType::TEXT);
    schema->addField("intVariable", BasicType::INT32);
    schema->addField("byteVariable", BasicType::INT8);
    schema->addField("shortVariable", BasicType::INT16);
    schema->addField("longVariable", BasicType::INT64);
    schema->addField("floatVariable", BasicType::FLOAT32);
    schema->addField("doubleVariable", BasicType::FLOAT64);
    schema->addField("booleanVariable", BasicType::BOOLEAN);
    auto memoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipeline = initPipelineOperator(schema, memoryLayout);

    auto buffer = bm->getBufferBlocking();
    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        std::string value = "X";
        auto varLengthBuffer = bm->getBufferBlocking();
        *varLengthBuffer.getBuffer<uint32_t>() = value.size();
        std::strcpy(varLengthBuffer.getBuffer<char>() + sizeof(uint32_t), value.c_str());
        auto strIndex = buffer.storeChildBuffer(varLengthBuffer);

        dynamicBuffer[i]["byteVariable"].write((int8_t) i);
        dynamicBuffer[i]["shortVariable"].write((int16_t) i);
        dynamicBuffer[i]["intVariable"].write((int32_t) i);
        dynamicBuffer[i]["longVariable"].write((int64_t) i);
        dynamicBuffer[i]["floatVariable"].write((float) i);
        dynamicBuffer[i]["doubleVariable"].write((double) i);
        dynamicBuffer[i]["booleanVariable"].write(true);
        dynamicBuffer[i]["stringVariable"].write(strIndex);
        dynamicBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler("stream.nebula.ComplexPojoMapFunction",
                                  "map",
                                  "stream.nebula.ComplexPojo",
                                  "stream.nebula.ComplexPojo",
                                  schema);

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resultDynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        EXPECT_EQ(resultDynamicBuffer[i]["byteVariable"].read<int8_t>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["shortVariable"].read<int16_t>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["intVariable"].read<int32_t>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["longVariable"].read<int64_t>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["floatVariable"].read<float>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["doubleVariable"].read<double>(), i + 10);
        EXPECT_EQ(resultDynamicBuffer[i]["booleanVariable"].read<bool>(), false);
        auto index = resultDynamicBuffer[i]["stringVariable"].read<uint32_t>();
        auto varLengthBuffer = resultBuffer.loadChildBuffer(index);
        auto textValue = varLengthBuffer.getBuffer<TextValue>();
        auto size = textValue->length();
        EXPECT_EQ(std::string(textValue->c_str(), size), "XAppended String:");
    }
}

INSTANTIATE_TEST_CASE_P(testMapJavaUDF,
                        MapJavaUDFPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<MapJavaUDFPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution