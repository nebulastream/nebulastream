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

#include <gtest/gtest.h>

#include <map>
#include <vector>

#include <NodeEngine/Execution/ExecutablePipelineStage.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>

#include <Util/Logger.hpp>
#include <cstdlib>
#include <iostream>

#include <API/Query.hpp>
#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/Execution/PipelineExecutionContext.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>

#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowActions/ExecutableSliceAggregationTriggerAction.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>

#include <Windowing/WindowingForwardRefs.hpp>

using namespace NES::Windowing;
namespace NES {
using NodeEngine::TupleBuffer;

class WindowManagerTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("WindowManagerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WindowMangerTest test class.");
    }

    void TearDown() { std::cout << "Tear down WindowManager test class." << std::endl; }

    const uint64_t buffers_managed = 10;
    const uint64_t buffer_size = 4 * 1024;
};

class MockedExecutablePipelineStage : public NodeEngine::Execution::ExecutablePipelineStage {
  public:
    static NodeEngine::Execution::ExecutablePipelineStagePtr create() {
        return std::make_shared<MockedExecutablePipelineStage>();
    }

    ExecutionResult execute(TupleBuffer&, NodeEngine::Execution::PipelineExecutionContext&, NodeEngine::WorkerContext&) override {
        return ExecutionResult::Ok;
    }
};

class MockedPipelineExecutionContext : public NodeEngine::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(NodeEngine::QueryManagerPtr queryManager,
                                   NodeEngine::BufferManagerPtr bufferManager,
                                   NodeEngine::Execution::OperatorHandlerPtr operatorHandler)
        : MockedPipelineExecutionContext(queryManager,
                                         bufferManager,
                                         std::vector<NodeEngine::Execution::OperatorHandlerPtr>{operatorHandler}) {}
    MockedPipelineExecutionContext(NodeEngine::QueryManagerPtr queryManager,
                                   NodeEngine::BufferManagerPtr bufferManager,
                                   std::vector<NodeEngine::Execution::OperatorHandlerPtr> operatorHandlers)
        : PipelineExecutionContext(
            0,
            queryManager,
            std::move(bufferManager),
            [this](TupleBuffer& buffer, NodeEngine::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            std::move(operatorHandlers),
            12){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

TEST_F(WindowManagerTest, testSumAggregation) {
    auto aggregation = ExecutableSumAggregation<int64_t>::create();
    auto partial = aggregation->lift(1L);
    auto partial2 = aggregation->lift(2L);
    auto combined = aggregation->combine(partial, partial2);
    auto result = aggregation->lower(combined);
    ASSERT_EQ(result, 3);
}

TEST_F(WindowManagerTest, testMaxAggregation) {
    auto aggregation = ExecutableMaxAggregation<int64_t>::create();
    auto partial = aggregation->lift(1L);
    auto partial2 = aggregation->lift(4L);
    auto combined = aggregation->combine(partial, partial2);
    auto result = aggregation->lower(combined);
    ASSERT_EQ(result, 4);
}

TEST_F(WindowManagerTest, testMinAggregation) {
    auto aggregation = ExecutableMinAggregation<int64_t>::create();

    auto partial = aggregation->lift(1L);
    auto partial2 = aggregation->lift(4L);
    auto combined = aggregation->combine(partial, partial2);
    auto result = aggregation->lower(combined);
    ASSERT_EQ(result, 1);
}

TEST_F(WindowManagerTest, testCountAggregation) {
    auto aggregation = ExecutableCountAggregation<int64_t>::create();
    auto partial = aggregation->lift(1L);
    auto partial2 = aggregation->lift(4L);
    auto combined = aggregation->combine(partial, partial2);
    auto result = aggregation->lower(combined);
    ASSERT_EQ(result, 2);
}

TEST_F(WindowManagerTest, testAvgAggregation) {
    auto aggregation = ExecutableAVGAggregation<int64_t>::create();
    auto partial = aggregation->lift(1L);
    auto partial2 = aggregation->lift(4L);
    auto combined = aggregation->combine(partial, partial2);
    auto result = aggregation->lower(combined);
    ASSERT_EQ(result, 2.5);
}

TEST_F(WindowManagerTest, testCheckSlice) {
    auto store = new WindowSliceStore<int64_t>(0L);
    auto aggregation = Sum(Attribute("value"));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef = Windowing::LogicalWindowDefinition::create(aggregation,
                                                                TumblingWindow::of(EventTime(Attribute("ts")), Seconds(60)),
                                                                DistributionCharacteristic::createCompleteWindowType(),
                                                                1,
                                                                trigger,
                                                                triggerAction,
                                                                0);

    auto windowManager = new WindowManager(windowDef->getWindowType(), 0, 1);
    uint64_t ts = 10;

    windowManager->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;

    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    // std::cout << aggregates[sliceIndex] << std::endl;
    // ASSERT_EQ(buffers_count, buffers_managed);
    ASSERT_EQ(aggregates[sliceIndex], 2);
}

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType, class sumType>
std::shared_ptr<Windowing::AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>>
createWindowHandler(Windowing::LogicalWindowDefinitionPtr windowDefinition,
                    SchemaPtr resultSchema,
                    PartialAggregateType partialAggregateInitialValue) {

    auto aggregation = sumType::create();
    auto trigger = Windowing::ExecutableOnTimeTriggerPolicy::create(1000);
    auto triggerAction =
        Windowing::ExecutableCompleteAggregationTriggerAction<KeyType, InputType, PartialAggregateType, FinalAggregateType>::
            create(windowDefinition, aggregation, resultSchema, 1, partialAggregateInitialValue);
    return Windowing::AggregationWindowHandler<KeyType, InputType, PartialAggregateType, FinalAggregateType>::create(
        windowDefinition,
        aggregation,
        trigger,
        triggerAction,
        1,
        partialAggregateInitialValue);
}

TEST_F(WindowManagerTest, testWindowTriggerCompleteWindowWithAvg) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31341, {streamConf});

    auto aggregation = Avg(Attribute("id", UINT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef =
        Windowing::LogicalWindowDefinition::create(Attribute("key", UINT64),
                                                   aggregation,
                                                   TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                   DistributionCharacteristic::createCompleteWindowType(),
                                                   0,
                                                   trigger,
                                                   triggerAction,
                                                   0);
    windowDef->setDistributionCharacteristic(DistributionCharacteristic::createCompleteWindowType());
    auto windowInputSchema = Schema::create();
    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", FLOAT64);

    AVGPartialType<uint64_t> avgInit = AVGPartialType<uint64_t>();

    auto windowHandler =
        createWindowHandler<uint64_t,
                            uint64_t,
                            AVGPartialType<uint64_t>,
                            AVGResultType,
                            Windowing::ExecutableAVGAggregation<uint64_t>>(windowDef, windowOutputSchema, avgInit);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);

    auto windowState = windowHandler->getTypedWindowState();
    auto keyRef = windowState->get(10);
    AVGPartialType<uint64_t> defaultValue = AVGPartialType<uint64_t>();
    keyRef.valueOrDefault(defaultValue);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex].addToSum(1);
    aggregates[sliceIndex].addToCount();
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    //    store->setLastWatermark(7);

    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex].addToSum(5);
    aggregates[sliceIndex].addToCount();
    std::cout << aggregates[sliceIndex].getSum() << std::endl;
    std::cout << aggregates[sliceIndex].getCount() << std::endl;

    ASSERT_EQ(aggregates[sliceIndex].getSum(), 5);
    ASSERT_EQ(aggregates[sliceIndex].getCount(), 1);
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto windowAction = std::dynamic_pointer_cast<
        Windowing::ExecutableCompleteAggregationTriggerAction<uint64_t, uint64_t, AVGPartialType<uint64_t>, AVGResultType>>(
        windowHandler->getWindowAction());
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, ts);

    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    //    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerCompleteWindow) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31341, {streamConf});

    auto aggregation = Sum(Attribute("id", UINT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef =
        Windowing::LogicalWindowDefinition::create(Attribute("key", UINT64),
                                                   aggregation,
                                                   TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                   DistributionCharacteristic::createCompleteWindowType(),
                                                   0,
                                                   trigger,
                                                   triggerAction,
                                                   0);
    windowDef->setDistributionCharacteristic(DistributionCharacteristic::createCompleteWindowType());
    auto windowInputSchema = Schema::create();
    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);

    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDef,
            windowOutputSchema,
            0);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);

    auto windowState = windowHandler->getTypedWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    //    store->setLastWatermark(7);

    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto windowAction =
        std::dynamic_pointer_cast<Windowing::ExecutableCompleteAggregationTriggerAction<uint64_t, uint64_t, uint64_t, uint64_t>>(
            windowHandler->getWindowAction());
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, ts);

    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerSlicingWindow) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31338, {streamConf});

    auto aggregation = Sum(Attribute("id", INT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef =
        Windowing::LogicalWindowDefinition::create(Attribute("key", INT64),
                                                   aggregation,
                                                   TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                   DistributionCharacteristic::createSlicingWindowType(),
                                                   0,
                                                   trigger,
                                                   triggerAction,
                                                   0);

    auto windowInputSchema = Schema::create();
    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", INT64)
                                  ->addField("value", INT64);
    auto windowHandler =
        createWindowHandler<int64_t, int64_t, int64_t, int64_t, Windowing::ExecutableSumAggregation<int64_t>>(windowDef,
                                                                                                              windowOutputSchema,
                                                                                                              0);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);

    auto windowState = windowHandler->getTypedWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    //    store->setLastWatermark(7);

    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
    auto windowAction =
        std::dynamic_pointer_cast<Windowing::ExecutableCompleteAggregationTriggerAction<int64_t, int64_t, int64_t, int64_t>>(
            windowHandler->getWindowAction());
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(11, store, windowDef, buf, ts, ts);

    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerCombiningWindow) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31339, {streamConf});
    auto aggregation = Sum(Attribute("id", INT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef = LogicalWindowDefinition::create(Attribute("key", INT64),
                                                     aggregation,
                                                     TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                     DistributionCharacteristic::createCombiningWindowType(),
                                                     1,
                                                     trigger,
                                                     triggerAction,
                                                     0);
    auto exec = ExecutableSumAggregation<int64_t>::create();

    auto windowInputSchema = Schema::create();
    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", INT64)
                                  ->addField("value", INT64);

    auto windowHandler =
        createWindowHandler<int64_t, int64_t, int64_t, int64_t, Windowing::ExecutableSumAggregation<int64_t>>(windowDef,
                                                                                                              windowOutputSchema,
                                                                                                              0);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);

    auto windowState =
        std::dynamic_pointer_cast<Windowing::AggregationWindowHandler<int64_t, int64_t, int64_t, int64_t>>(windowHandler)
            ->getTypedWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    //    store->setLastWatermark(7);

    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);

    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto windowAction = ExecutableCompleteAggregationTriggerAction<int64_t, int64_t, int64_t, int64_t>::create(windowDef,
                                                                                                               exec,
                                                                                                               windowOutputSchema,
                                                                                                               1,
                                                                                                               0);
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(11, store, windowDef, buf, ts, ts);
    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerCompleteWindowCheckRemoveSlices) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, {streamConf});

    auto aggregation = Sum(Attribute("id", UINT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDef =
        Windowing::LogicalWindowDefinition::create(Attribute("key", UINT64),
                                                   aggregation,
                                                   TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                   DistributionCharacteristic::createCompleteWindowType(),
                                                   0,
                                                   trigger,
                                                   triggerAction,
                                                   0);
    windowDef->setDistributionCharacteristic(DistributionCharacteristic::createCompleteWindowType());

    auto windowInputSchema = Schema::create();
    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", UINT64)
                                  ->addField("value", UINT64);

    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDef,
            windowOutputSchema,
            0);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);
    auto windowState = windowHandler->getTypedWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto windowAction =
        std::dynamic_pointer_cast<Windowing::ExecutableCompleteAggregationTriggerAction<uint64_t, uint64_t, uint64_t, uint64_t>>(
            windowHandler->getWindowAction());
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, ts);

    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);

    ASSERT_EQ(store->getSliceMetadata().size(), 2);
    ASSERT_EQ(store->getPartialAggregates().size(), 2);
}

TEST_F(WindowManagerTest, testWindowTriggerSlicingWindowCheckRemoveSlices) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31340, {streamConf});

    auto aggregation = Sum(Attribute("id", INT64));
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowInputSchema = Schema::create();

    auto windowDef =
        Windowing::LogicalWindowDefinition::create(Attribute("key", INT64),
                                                   aggregation,
                                                   TumblingWindow::of(EventTime(Attribute("value")), Milliseconds(10)),
                                                   DistributionCharacteristic::createSlicingWindowType(),
                                                   0,
                                                   trigger,
                                                   triggerAction,
                                                   0);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("start", UINT64))
                                  ->addField(createField("end", UINT64))
                                  ->addField("key", INT64)
                                  ->addField("value", INT64);

    auto windowHandler =
        createWindowHandler<int64_t, int64_t, int64_t, int64_t, Windowing::ExecutableSumAggregation<int64_t>>(windowDef,
                                                                                                              windowOutputSchema,
                                                                                                              0);
    windowHandler->start(nodeEngine->getStateManager());
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDef, windowOutputSchema, windowHandler);
    auto context = std::make_shared<MockedPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                    nodeEngine->getBufferManager(),
                                                                    windowOperatorHandler);

    windowHandler->setup(context);

    auto windowState = windowHandler->getTypedWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    windowHandler->setLastWatermark(7);
    store->incrementRecordCnt(sliceIndex);
    //    store->setLastWatermark(7);

    ts = 14;
    windowHandler->updateMaxTs(ts, 0);
    windowHandler->getWindowManager()->sliceStream(ts, store, 0);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);
    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
    auto windowAction =
        std::dynamic_pointer_cast<Windowing::ExecutableCompleteAggregationTriggerAction<int64_t, int64_t, int64_t, int64_t>>(
            windowHandler->getWindowAction());
    windowAction->aggregateWindows(10, store, windowDef, buf, ts, 7);
    windowAction->aggregateWindows(11, store, windowDef, buf, ts, ts);

    uint64_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3]
              << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);

    ASSERT_EQ(store->getSliceMetadata().size(), 2);
    ASSERT_EQ(store->getPartialAggregates().size(), 2);
}

}// namespace NES
