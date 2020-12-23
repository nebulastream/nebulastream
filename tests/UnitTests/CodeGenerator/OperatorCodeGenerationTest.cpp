/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <QueryCompiler/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CCodeGenerator/Definitions/FunctionDefinition.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/IFStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/ReturnStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/UnaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarDeclStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/VarRefStatement.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <QueryCompiler/CompilerTypesFactory.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratedCode.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <State/StateVariable.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <cassert>
#include <cmath>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

using std::cout;
using std::endl;
namespace NES {

class OperatorCodeGenerationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("OperatorOperatorCodeGenerationTest.log", NES::LOG_DEBUG);
        std::cout << "Setup OperatorOperatorCodeGenerationTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() { std::cout << "Setup OperatorOperatorCodeGenerationTest test case." << std::endl; }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Tear down OperatorOperatorCodeGenerationTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down OperatorOperatorCodeGenerationTest test class." << std::endl; }
};

class TestPipelineExecutionContext : public NodeEngine::Execution::PipelineExecutionContext {
  public:
    TestPipelineExecutionContext(NodeEngine::BufferManagerPtr bufferManager,
                                 NodeEngine::Execution::OperatorHandlerPtr operatorHandler)
        : TestPipelineExecutionContext(bufferManager, std::vector<NodeEngine::Execution::OperatorHandlerPtr>{operatorHandler}) {}
    TestPipelineExecutionContext(NodeEngine::BufferManagerPtr bufferManager,
                                 std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers)
        : PipelineExecutionContext(
            0, std::move(bufferManager),
            [this](TupleBuffer& buffer, NodeEngine::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            std::move(operatorHandlers)){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

const DataSourcePtr createTestSourceCodeGen(NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr) {
    return std::make_shared<DefaultSource>(Schema::create()->addField("campaign_id", DataTypeFactory::createInt64()), bPtr, dPtr,
                                           1, 1, 1);
}

class SelectionDataGenSource : public GeneratorSource {
  public:
    SelectionDataGenSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr,
                           const uint64_t pNum_buffers_to_process)
        : GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process, 1) {}

    ~SelectionDataGenSource() = default;

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = buf.getBufferSize() / sizeof(InputTuple);

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();
        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].value = i * 2;
            for (int j = 0; j < 11; ++j) {
                tuples[i].text[j] = ((j + i) % (255 - 'a')) + 'a';
            }
            tuples[i].text[12] = '\0';
        }

        //buf.setBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
        char text[12];
    };
};

const DataSourcePtr createTestSourceCodeGenFilter(NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr) {
    DataSourcePtr source(std::make_shared<SelectionDataGenSource>(Schema::create()
                                                                      ->addField("id", DataTypeFactory::createUInt32())
                                                                      ->addField("value", DataTypeFactory::createUInt32())
                                                                      ->addField("text", DataTypeFactory::createFixedChar(12)),
                                                                  bPtr, dPtr, 1));

    return source;
}

class PredicateTestingDataGeneratorSource : public GeneratorSource {
  public:
    PredicateTestingDataGeneratorSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr,
                                        const uint64_t pNum_buffers_to_process)
        : GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process, 1) {}

    ~PredicateTestingDataGeneratorSource() = default;

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        int16_t valueSmall;
        float valueFloat;
        double valueDouble;
        char singleChar;
        char text[12];
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = buf.getBufferSize() / sizeof(InputTuple);

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].id = i;
            tuples[i].valueSmall = -123 + (i * 2);
            tuples[i].valueFloat = i * M_PI;
            tuples[i].valueDouble = i * M_PI * 2;
            tuples[i].singleChar = ((i + 1) % (127 - 'A')) + 'A';
            for (int j = 0; j < 11; ++j) {
                tuples[i].text[j] = ((i + 1) % 64) + 64;
            }
            tuples[i].text[11] = '\0';
        }

        //buf->setBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }
};

const DataSourcePtr createTestSourceCodeGenPredicate(NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr) {
    DataSourcePtr source(
        std::make_shared<PredicateTestingDataGeneratorSource>(Schema::create()
                                                                  ->addField("id", DataTypeFactory::createUInt32())
                                                                  ->addField("valueSmall", DataTypeFactory::createInt16())
                                                                  ->addField("valueFloat", DataTypeFactory::createFloat())
                                                                  ->addField("valueDouble", DataTypeFactory::createDouble())
                                                                  ->addField("valueChar", DataTypeFactory::createChar())
                                                                  ->addField("text", DataTypeFactory::createFixedChar(12)),
                                                              bPtr, dPtr, 1));

    return source;
}

class WindowTestingDataGeneratorSource : public GeneratorSource {
  public:
    WindowTestingDataGeneratorSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr,
                                     const uint64_t pNum_buffers_to_process)
        : GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process, 1) {}

    ~WindowTestingDataGeneratorSource() = default;

    struct __attribute__((packed)) InputTuple {
        uint64_t key;
        uint64_t value;
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = 10;

        assert(buf.getBuffer() != NULL);

        InputTuple* tuples = (InputTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].key = i % 2;
            tuples[i].value = 1;
        }

        //bufsetBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }
};

class WindowTestingWindowGeneratorSource : public GeneratorSource {
  public:
    WindowTestingWindowGeneratorSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr,
                                       const uint64_t pNum_buffers_to_process)
        : GeneratorSource(schema, bPtr, dPtr, pNum_buffers_to_process, 1) {}

    ~WindowTestingWindowGeneratorSource() = default;

    struct __attribute__((packed)) WindowTuple {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t value;
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = 10;

        assert(buf.getBuffer() != NULL);

        WindowTuple* tuples = (WindowTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].start = i;
            tuples[i].end = i * 2;
            tuples[i].key = 1;
            tuples[i].value = 1;
        }

        //bufsetBufferSizeInBytes(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }
};

const DataSourcePtr createWindowTestDataSource(NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr) {
    DataSourcePtr source(std::make_shared<WindowTestingDataGeneratorSource>(
        Schema::create()->addField("key", DataTypeFactory::createUInt64())->addField("value", DataTypeFactory::createUInt64()),
        bPtr, dPtr, 10));
    return source;
}

const DataSourcePtr createWindowTestSliceSource(NodeEngine::BufferManagerPtr bPtr, NodeEngine::QueryManagerPtr dPtr,
                                                SchemaPtr schema) {
    DataSourcePtr source(std::make_shared<WindowTestingWindowGeneratorSource>(schema, bPtr, dPtr, 10));
    return source;
}

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType, class sumType>
std::shared_ptr<Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>>
createWindowHandler(Windowing::LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema) {

    auto aggregation = sumType::create();
    auto trigger = Windowing::ExecutableOnTimeTriggerPolicy::create(1000);
    auto triggerAction = Windowing::ExecutableCompleteAggregationTriggerAction<uint64_t, uint64_t, uint64_t, uint64_t>::create(
        windowDefinition, aggregation, resultSchema);
    return Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>::create(windowDefinition, aggregation,
                                                                                               trigger, triggerAction);
}

/**
 * @brief This test generates a simple copy function, which copies code from one buffer to another
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCopy) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    auto source = createTestSourceCodeGen(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    context->pipelineName = "1";
    NES_INFO("Generate Code");
    /* generate code for scanning input buffer */
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context);
    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(Schema::create()->addField("campaign_id", DataTypeFactory::createUInt64()), context);
    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(context);

    /* prepare input and output tuple buffer */
    auto schema = Schema::create()->addField("i64", DataTypeFactory::createUInt64());
    auto buffer = source->receiveData().value();

    /* execute Stage */
    NES_INFO("Processing " << buffer.getNumberOfTuples() << " tuples: ");
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(),
                                                                       std::vector<NodeEngine::Execution::OperatorHandlerPtr>());
    NodeEngine::WorkerContext wctx{0};
    stage->setup(*queryContext.get());
    stage->start(*queryContext.get());
    stage->execute(buffer, *queryContext.get(), wctx);
    auto resultBuffer = queryContext->buffers[0];
    /* check for correctness, input source produces uint64_t tuples and stores a 1 in each tuple */
    EXPECT_EQ(buffer.getNumberOfTuples(), resultBuffer.getNumberOfTuples());
    auto layout = NodeEngine::createRowLayout(schema);
    for (uint64_t recordIndex = 0; recordIndex < buffer.getNumberOfTuples(); ++recordIndex) {
        EXPECT_EQ(1, layout->getValueField<uint64_t>(recordIndex, /*fieldIndex*/ 0)->read(buffer));
    }
}
/**
 * @brief This test generates a predicate, which filters elements in the input buffer
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationFilterPredicate) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    auto source = createTestSourceCodeGenFilter(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();

    /* generate code for scanning input buffer */
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context);

    auto pred = std::dynamic_pointer_cast<Predicate>((PredicateItem(inputSchema->get(0)) < PredicateItem(
                                                          DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "5")))
                                                         .copy());

    codeGenerator->generateCodeForFilter(pred, context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(source->getSchema(), context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(context);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();
    NES_INFO("Processing " << inputBuffer.getNumberOfTuples() << " tuples: ");

    /* execute Stage */
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(),
                                                                       std::vector<NodeEngine::Execution::OperatorHandlerPtr>());
    NodeEngine::WorkerContext wctx{0};
    stage->setup(*queryContext.get());
    stage->start(*queryContext.get());
    stage->execute(inputBuffer, *queryContext.get(), wctx);
    auto resultBuffer = queryContext->buffers[0];
    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    NES_INFO("Number of generated output tuples: " << resultBuffer.getNumberOfTuples());
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

    auto resultData = (SelectionDataGenSource::InputTuple*) resultBuffer.getBuffer();
    for (uint64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(resultData[i].id, i);
        EXPECT_EQ(resultData[i].value, i * 2);
    }
}

TEST_F(OperatorCodeGenerationTest, codeGenerationScanOperator) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationWindowAssigner) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);

    auto trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", BasicType::UINT64), sum, TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);
    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());

    auto strategy = EventTimeWatermarkStrategy::create(windowDefinition->getOnKey(), 12, 1);
    codeGenerator->generateCodeForWatermarkAssigner(strategy, context1);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    context2->pipelineName = "1";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCompleteWindowIngestionTime) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", BasicType::UINT64), sum, TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);
    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    context2->pipelineName = "1";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition, windowOutputSchema);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);
    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);

    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    // auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowHandler, nullptr);
    stage1->execute(inputBuffer, *executionContext.get(), wctx);

    //check partial aggregates in window state
    auto stateVar = windowHandler->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCompleteWindowEventTime) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();

    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition =
        LogicalWindowDefinition::create(Attribute("key", BasicType::UINT64), sum,
                                        TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("value")), Seconds(10)),
                                        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);
    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition, windowOutputSchema);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);
    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);

    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    stage1->execute(inputBuffer, *executionContext.get(), wctx);

    //check partial aggregates in window state
    auto stateVar = windowHandler->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCompleteWindowEventTimeWithTimeUnit) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();

    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", BasicType::UINT64), sum,
        TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("value"), Seconds()), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);
    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);

    // init window handler

    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition, windowOutputSchema);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);
    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);
    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);
    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);
    stage1->setup(*queryContext.get());
    stage1->start(*queryContext.get());
    stage1->execute(inputBuffer, *queryContext.get(), wctx);

    //check partial aggregates in window state
    auto stateVar =
        windowHandler->as<Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>>()->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5);
}

/**
 * @brief This test generates a window slicer
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationDistributedSlicer) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    auto trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto sum = SumAggregationDescriptor::on(Attribute("value", BasicType::UINT64));
    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", BasicType::UINT64), sum, TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);

    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForSlicingWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);
    // init window handler
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition, windowOutputSchema);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);

    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);
    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    stage1->setup(*executionContext.get());
    stage1->start(*executionContext.get());
    stage1->execute(inputBuffer, *executionContext.get(), wctx);

    //check partial aggregates in window state
    auto stateVar = windowHandler->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationDistributedCombiner) {
    /* prepare objects for test */
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    auto schema = Schema::create()
                      ->addField(createField("start", UINT64))
                      ->addField(createField("end", UINT64))
                      ->addField(createField("cnt", UINT64))
                      ->addField(createField("key", UINT64))
                      ->addField("value", UINT64);

    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    codeGenerator->generateCodeForScan(schema, schema, context1);
    auto trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", UINT64), sum, TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);

    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForCombiningWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(context1);

    auto context2 = PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(schema, schema, context2);
    auto stage2 = codeGenerator->compile(context2);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);

    // init window handler
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition, windowOutputSchema);

    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);
    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), windowOperatorHandler);

    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);
    windowHandler->setup(executionContext);

    auto layout = NodeEngine::createRowLayout(schema);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    layout->getValueField<uint64_t>(0, 0)->write(buffer, 100);//start 100
    layout->getValueField<uint64_t>(0, 1)->write(buffer, 110);//stop 200
    layout->getValueField<uint64_t>(0, 2)->write(buffer, 1);  //cnt
    layout->getValueField<uint64_t>(0, 3)->write(buffer, 1);  //key 1
    layout->getValueField<uint64_t>(0, 4)->write(buffer, 10); //value 10
    buffer.setNumberOfTuples(1);

    layout->getValueField<uint64_t>(1, 0)->write(buffer, 100);//start 100
    layout->getValueField<uint64_t>(1, 1)->write(buffer, 110);//stop 200
    layout->getValueField<uint64_t>(0, 2)->write(buffer, 1);  //cnt
    layout->getValueField<uint64_t>(1, 3)->write(buffer, 1);  //key 1
    layout->getValueField<uint64_t>(1, 4)->write(buffer, 8);  //value 10
    buffer.setNumberOfTuples(2);

    layout->getValueField<uint64_t>(2, 0)->write(buffer, 100);//start 100
    layout->getValueField<uint64_t>(2, 1)->write(buffer, 110);//stop 200
    layout->getValueField<uint64_t>(0, 2)->write(buffer, 1);  //cnt
    layout->getValueField<uint64_t>(2, 3)->write(buffer, 1);  //key 1
    layout->getValueField<uint64_t>(2, 4)->write(buffer, 2);  //value 10
    buffer.setNumberOfTuples(3);

    layout->getValueField<uint64_t>(3, 0)->write(buffer, 200);//start 100
    layout->getValueField<uint64_t>(3, 1)->write(buffer, 210);//stop 200
    layout->getValueField<uint64_t>(0, 2)->write(buffer, 1);  //cnt
    layout->getValueField<uint64_t>(3, 3)->write(buffer, 3);  //key 1
    layout->getValueField<uint64_t>(3, 4)->write(buffer, 2);  //value 10
    buffer.setNumberOfTuples(4);

    layout->getValueField<uint64_t>(4, 0)->write(buffer, 200);//start 100
    layout->getValueField<uint64_t>(4, 1)->write(buffer, 210);//stop 200
    layout->getValueField<uint64_t>(0, 2)->write(buffer, 1);  //cnt
    layout->getValueField<uint64_t>(4, 3)->write(buffer, 5);  //key 1
    layout->getValueField<uint64_t>(4, 4)->write(buffer, 12); //value 10
    buffer.setNumberOfTuples(5);

    std::cout << "buffer=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, schema) << std::endl;
    /* execute Stage */
    stage1->setup(*executionContext.get());
    stage1->start(*executionContext.get());
    stage1->execute(buffer, *executionContext.get(), wctx);

    //check partial aggregates in window state
    auto stateVar = windowHandler->getTypedWindowState();
    std::vector<uint64_t> results;
    for (auto& [key, val] : stateVar->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        for (auto& slice : val->getSliceMetadata()) {
            std::cout << "start=" << slice.getStartTs() << " end=" << slice.getEndTs() << std::endl;
            results.push_back(slice.getStartTs());
            results.push_back(slice.getEndTs());
        }
        for (auto& agg : val->getPartialAggregates()) {
            std::cout << "key=" << key << std::endl;
            results.push_back(key);
            std::cout << "value=" << agg << std::endl;
            results.push_back(agg);
        }
    }

    EXPECT_EQ(results[0], 100);
    EXPECT_EQ(results[1], 110);
    EXPECT_EQ(results[2], 1);
    EXPECT_EQ(results[3], 20);

    EXPECT_EQ(results[4], 200);
    EXPECT_EQ(results[5], 210);
    EXPECT_EQ(results[6], 3);
    EXPECT_EQ(results[7], 2);

    EXPECT_EQ(results[8], 200);
    EXPECT_EQ(results[9], 210);
    EXPECT_EQ(results[10], 5);
    EXPECT_EQ(results[11], 12);
}

TEST_F(OperatorCodeGenerationTest, codeGenerationTriggerWindowOnRecord) {
    /* prepare objects for test */
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);
    auto schema = Schema::create()
                      ->addField(createField("start", UINT64))
                      ->addField(createField("end", UINT64))
                      ->addField(createField("cnt", UINT64))
                      ->addField(createField("key", UINT64))
                      ->addField("value", UINT64);

    auto codeGenerator = CCodeGenerator::create();
    auto context1 = PipelineContext::create();
    context1->pipelineName = "1";
    codeGenerator->generateCodeForScan(schema, schema, context1);
    auto trigger = OnRecordTriggerPolicyDescription::create();

    auto sum = SumAggregationDescriptor::on(Attribute("value", UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDefinition = LogicalWindowDefinition::create(
        Attribute("key", UINT64), sum, TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)),
        DistributionCharacteristic::createCompleteWindowType(), 1, trigger, triggerAction);

    auto aggregate =
        TranslateToGeneratableOperatorPhase::create()->transformWindowAggregation(windowDefinition->getWindowAggregation());
    codeGenerator->generateCodeForCombiningWindow(windowDefinition, aggregate, context1, 0);
    std::string codeString = codeGenerator->generateCode(context1);

    auto found = codeString.find("windowHandler->trigger();");
    cout << "code=" << codeString << std::endl;
    EXPECT_NE(found, std::string::npos);
}

/**
 * @brief This test generates a predicate with string comparision
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationStringComparePredicateTest) {
    // auto str = strcmp("HHHHHHHHHHH", {'H', 'V'});
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    codeGenerator->generateCodeForScan(inputSchema, inputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForFilter(
        createPredicate((inputSchema->get(2) > 30.4) && (inputSchema->get(4) == 'F' || (inputSchema->get(5) == "HHHHHHHHHHH"))),
        context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(inputSchema, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(context);

    /* prepare input tuple buffer */
    auto optVal = source->receiveData();
    NES_ASSERT(optVal.has_value(), "invalid buffer");
    auto inputBuffer = *optVal;

    /* execute Stage */

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(),
                                                                       std::vector<NodeEngine::Execution::OperatorHandlerPtr>());
    cout << "inputBuffer=" << UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, inputSchema) << endl;
    NodeEngine::WorkerContext wctx{0};
    stage->setup(*queryContext.get());
    stage->start(*queryContext.get());
    stage->execute(inputBuffer, *queryContext.get(), wctx);

    auto resultBuffer = queryContext->buffers[0];

    /* check for correctness, input source produces tuples consisting of two uint32_t values, 3 values will match the predicate */
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 3);

    NES_INFO(UtilityFunctions::prettyPrintTupleBuffer(resultBuffer, inputSchema));
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationMapPredicateTest) {
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto context = PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create()
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(mappedValue, createPredicate((inputSchema->get(2) * inputSchema->get(3)) + 2), context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(context);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    NodeEngine::WorkerContext wctx{0};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(),
                                                                       std::vector<NodeEngine::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext.get());
    stage->start(*queryContext.get());
    stage->execute(inputBuffer, *queryContext.get(), wctx);

    auto resultBuffer = queryContext->buffers[0];

    auto inputLayout = NodeEngine::createRowLayout(inputSchema);
    auto outputLayout = NodeEngine::createRowLayout(outputSchema);
    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = inputLayout->getValueField<float>(recordIndex, /*fieldIndex*/ 2)->read(inputBuffer);
        auto doubleValue = inputLayout->getValueField<double>(recordIndex, /*fieldIndex*/ 3)->read(inputBuffer);
        auto reference = (floatValue * doubleValue) + 2;
        auto mapedValue = outputLayout->getValueField<double>(recordIndex, /*fieldIndex*/ 4)->read(resultBuffer);
        EXPECT_EQ(reference, mapedValue);
    }
}

/**
 * @brief This test generates a window slicer
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationJoin) {
    /* prepare objects for test */
    auto streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 6116, streamConf);

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = CCodeGenerator::create();
    auto pipelineContext1 = PipelineContext::create();
    pipelineContext1->pipelineName = "1";
    auto input_schema = source->getSchema();

    WindowTriggerPolicyPtr triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
    auto distrType = DistributionCharacteristic::createCompleteWindowType();
    Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "key")->as<FieldAccessExpressionNode>(),
        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)), distrType, triggerPolicy, triggerAction,
        1, 1);

    joinDef->updateStreamTypes(input_schema, input_schema);

    auto outputSchema = Schema::create()
                            ->addField(createField("start", UINT64))
                            ->addField(createField("end", UINT64))
                            ->addField(AttributeField::create("key", joinDef->getLeftJoinKey()->getStamp()));
    for (auto field : input_schema->fields) {
        outputSchema = outputSchema->addField("left_" + field->name, field->getDataType());
    }
    for (auto field : input_schema->fields) {
        outputSchema = outputSchema->addField("right_" + field->name, field->getDataType());
    }
    joinDef->updateOutputDefinition(outputSchema);
    auto joinOperatorHandler = Join::JoinOperatorHandler::create(joinDef, source->getSchema());

    pipelineContext1->registerOperatorHandler(joinOperatorHandler);
    pipelineContext1->arity = PipelineContext::BinaryLeft;
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), pipelineContext1);
    auto index = codeGenerator->generateJoinSetup(joinDef, pipelineContext1);
    codeGenerator->generateCodeForJoin(joinDef, pipelineContext1, index);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(pipelineContext1);

    auto context2 = PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(context2);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getBufferManager(), joinOperatorHandler);
    auto nextPipeline = NodeEngine::Execution::ExecutablePipeline::create(1, 0, stage2, executionContext, nullptr);

    auto context3 = PipelineContext::create();
    context3->registerOperatorHandler(joinOperatorHandler);
    context3->arity = PipelineContext::BinaryRight;
    context3->pipelineName = "3";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context3);
    codeGenerator->generateCodeForJoin(joinDef, context3, 0);
    auto stage3 = codeGenerator->compile(context3);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();
    NES_INFO("Processing " << inputBuffer.getNumberOfTuples() << " tuples: ");
    cout << "buffer content=" << UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, input_schema);

    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    stage1->setup(*executionContext.get());
    stage1->start(*executionContext.get());
    executionContext->getOperatorHandlers()[0]->start(executionContext);
    executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
        ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
        ->start();
    stage1->execute(inputBuffer, *executionContext.get(), wctx);
    stage3->execute(inputBuffer, *executionContext.get(), wctx);

    auto stateVarLeft = executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
                            ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
                            ->getLeftJoinState();
    auto stateVarRight = executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
                             ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
                             ->getRightJoinState();
    std::vector<int64_t> results;
    for (auto& [key, val] : stateVarLeft->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        auto lock = std::unique_lock(val->mutex());
        for (auto& list : val->getAppendList()) {
            for (auto& value : list) {
                results.push_back(value);
            }
        }
    }
    for (auto& [key, val] : stateVarRight->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        auto lock = std::unique_lock(val->mutex());
        for (auto& list : val->getAppendList()) {
            for (auto& value : list) {
                results.push_back(value);
            }
        }
    }
    EXPECT_EQ(results.size(), 40);
}
}// namespace NES
