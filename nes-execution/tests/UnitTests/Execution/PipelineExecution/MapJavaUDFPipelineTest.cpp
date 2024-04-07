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
#include <Operators/LogicalOperators/UDFs/JavaUDFDescriptor.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <Util/JavaUDFDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
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
        auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
        for (uint64_t i = 0; i < 10; i++) {
            testBuffer[i][variableName].write((T) i);
            testBuffer.setNumberOfTuples(i + 1);
        }
        return buffer;
    }

    /**
     * Initializes the map handler for the given pipeline.
     * @param javaUDFDescriptor Descriptor for the Java UDF under test.
     * @return operator handler
     */
    auto initMapHandler(const Catalogs::UDF::JavaUdfDescriptorPtr javaUDFDescriptor) {
        jni::JavaSerializedInstance instance;
        return std::make_shared<Operators::JavaUDFOperatorHandler>(javaUDFDescriptor->getClassName(),
                                                                   javaUDFDescriptor->getMethodName(),
                                                                   javaUDFDescriptor->getInputClassName(),
                                                                   javaUDFDescriptor->getOutputClassName(),
                                                                   javaUDFDescriptor->getByteCodeList(),
                                                                   instance,
                                                                   javaUDFDescriptor->getInputSchema(),
                                                                   javaUDFDescriptor->getOutputSchema(),
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

        auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
        for (uint64_t i = 0; i < 10; i++) {
            ASSERT_EQ(resulttestBuffer[i][variableName].read<T>(), i + 10);
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
    auto handler =
        initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                           .setClassName("stream.nebula.IntegerMapFunction")
                           .setMethodName("map")
                           .setInputClassName("java.lang.Integer")
                           .setOutputClassName("java.lang.Integer")
                           .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.IntegerMapFunction", {}}})
                           .setInputSchema(schema)
                           .setOutputSchema(schema)
                           .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                           .build());
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
    auto handler =
        initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                           .setClassName("stream.nebula.ShortMapFunction")
                           .setMethodName("map")
                           .setInputClassName("java.lang.Short")
                           .setOutputClassName("java.lang.Short")
                           .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.ShortMapFunction", {}}})
                           .setInputSchema(schema)
                           .setOutputSchema(schema)
                           .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                           .build());
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
    auto handler = initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                                      .setClassName("stream.nebula.ByteMapFunction")
                                      .setMethodName("map")
                                      .setInputClassName("java.lang.Byte")
                                      .setOutputClassName("java.lang.Byte")
                                      .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.ByteMapFunction", {}}})
                                      .setInputSchema(schema)
                                      .setOutputSchema(schema)
                                      .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                                      .build());
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
    auto handler = initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                                      .setClassName("stream.nebula.LongMapFunction")
                                      .setMethodName("map")
                                      .setInputClassName("java.lang.Long")
                                      .setOutputClassName("java.lang.Long")
                                      .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.LongMapFunction", {}}})
                                      .setInputSchema(schema)
                                      .setOutputSchema(schema)
                                      .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                                      .build());
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
    auto handler =
        initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                           .setClassName("stream.nebula.DoubleMapFunction")
                           .setMethodName("map")
                           .setInputClassName("java.lang.Double")
                           .setOutputClassName("java.lang.Double")
                           .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.DoubleMapFunction", {}}})
                           .setInputSchema(schema)
                           .setOutputSchema(schema)
                           .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                           .build());
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
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        testBuffer[i][variableName].write((bool) true);
        testBuffer.setNumberOfTuples(i + 1);
    }
    auto executablePipeline = provider->create(pipeline, options);
    auto handler =
        initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                           .setClassName("stream.nebula.BooleanMapFunction")
                           .setMethodName("map")
                           .setInputClassName("java.lang.Boolean")
                           .setOutputClassName("java.lang.Boolean")
                           .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.BooleanMapFunction", {}}})
                           .setInputSchema(schema)
                           .setOutputSchema(schema)
                           .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                           .build());
    auto pipelineContext = MockedPipelineExecutionContext({handler});

    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resulttestBuffer[i][variableName].read<bool>(), false);
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
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        testBuffer[i].writeVarSized("stringVariable", "X", bm.get());
        testBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);
    auto handler =
        initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                           .setClassName("stream.nebula.StringMapFunction")
                           .setMethodName("map")
                           .setInputClassName("java.lang.String")
                           .setOutputClassName("java.lang.String")
                           .setByteCodeList({{"stream.nebula.MapFunction", {}}, {"stream.nebula.StringMapFunction", {}}})
                           .setInputSchema(schema)
                           .setOutputSchema(schema)
                           .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                           .build());

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        ASSERT_EQ(resulttestBuffer[i].readVarSized("stringVariable"), "Appended String:X");
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
    auto testBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, buffer);
    for (uint64_t i = 0; i < 10; i++) {
        testBuffer[i]["byteVariable"].write((int8_t) i);
        testBuffer[i]["shortVariable"].write((int16_t) i);
        testBuffer[i]["intVariable"].write((int32_t) i);
        testBuffer[i]["longVariable"].write((int64_t) i);
        testBuffer[i]["floatVariable"].write((float) i);
        testBuffer[i]["doubleVariable"].write((double) i);
        testBuffer[i]["booleanVariable"].write(true);
        testBuffer[i].writeVarSized("stringVariable", "X", bm.get());
        testBuffer.setNumberOfTuples(i + 1);
    }

    auto executablePipeline = provider->create(pipeline, options);
    auto handler = initMapHandler(Catalogs::UDF::JavaUDFDescriptorBuilder()
                                      .setClassName("stream.nebula.ComplexPojoMapFunction")
                                      .setMethodName("map")
                                      .setInputClassName("stream.nebula.ComplexPojo")
                                      .setOutputClassName("stream.nebula.ComplexPojo")
                                      .setByteCodeList({{"stream.nebula.MapFunction", {}},
                                                        {"stream.nebula.ComplexPojo", {}},
                                                        {"stream.nebula.ComplexPojoMapFunction", {}}})
                                      .setInputSchema(schema)
                                      .setOutputSchema(schema)
                                      .loadByteCodeFrom(JAVA_UDF_TEST_DATA)
                                      .build());

    auto pipelineContext = MockedPipelineExecutionContext({handler});
    executablePipeline->setup(pipelineContext);
    executablePipeline->execute(buffer, pipelineContext, *wc);
    executablePipeline->stop(pipelineContext);

    ASSERT_EQ(pipelineContext.buffers.size(), 1);
    auto resultBuffer = pipelineContext.buffers[0];
    ASSERT_EQ(resultBuffer.getNumberOfTuples(), 10);

    auto resulttestBuffer = Runtime::MemoryLayouts::TestTupleBuffer(memoryLayout, resultBuffer);
    for (uint64_t i = 0; i < 10; i++) {
        EXPECT_EQ(resulttestBuffer[i]["byteVariable"].read<int8_t>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["shortVariable"].read<int16_t>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["intVariable"].read<int32_t>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["longVariable"].read<int64_t>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["floatVariable"].read<float>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["doubleVariable"].read<double>(), i + 10);
        EXPECT_EQ(resulttestBuffer[i]["booleanVariable"].read<bool>(), false);
        EXPECT_EQ(resulttestBuffer[i].readVarSized("stringVariable"), "XAppended String:");
    }
}

INSTANTIATE_TEST_CASE_P(testMapJavaUDF,
                        MapJavaUDFPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<MapJavaUDFPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution