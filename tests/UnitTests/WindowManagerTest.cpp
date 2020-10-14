#include <gtest/gtest.h>

#include <map>
#include <vector>

#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <State/StateManager.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/ExecutableAVGAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMaxAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableMinAggregation.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>

#include <Util/Logger.hpp>
#include <cstdlib>
#include <iostream>

#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/ExecutablePipeline.hpp>
#include <QueryCompiler/PipelineExecutionContext.hpp>
#include <Windowing/Runtime/WindowHandler.hpp>
#include <Windowing/Runtime/WindowHandlerFactory.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowAggregations/ExecutableCountAggregation.hpp>

namespace NES {
class WindowManagerTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("WindowManagerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WindowMangerTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down WindowManager test class." << std::endl;
    }

    const size_t buffers_managed = 10;
    const size_t buffer_size = 4 * 1024;
};

class TestAggregation : public WindowAggregationDescriptor {
  public:
    TestAggregation() : WindowAggregationDescriptor(){};
    void compileLiftCombine(CompoundStatementPtr, BinaryOperatorStatement,
                            StructDeclaration, BinaryOperatorStatement){};
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

TEST_F(WindowManagerTest, testCheckSlice) {
    auto maxAggregation = ExecutableAVGAggregation<int64_t>::create();

    auto store = new WindowSliceStore<int64_t>(0L);
    auto aggregation = SumAggregationDescriptor::on(nullptr);

    auto windowDef = std::make_shared<LogicalWindowDefinition>(
        LogicalWindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("ts")), Seconds(60)), DistributionCharacteristic::createCompleteWindowType()));

    auto windowManager = new WindowManager(windowDef);
    uint64_t ts = 10;

    windowManager->sliceStream(ts, store);
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

TEST_F(WindowManagerTest, testWindowTriggerCompleteWindow) {
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);

    auto aggregation = SumAggregationDescriptor::on(Attribute("id"));

    auto windowDef = std::make_shared<LogicalWindowDefinition>(
        LogicalWindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("value")), Milliseconds(10)), DistributionCharacteristic::createCompleteWindowType()));
    windowDef->setDistributionCharacteristic(DistributionCharacteristic::createCompleteWindowType());

    auto w = WindowHandlerFactory::create<uint64_t, uint64_t, uint64_t, uint64_t>(windowDef, ExecutableSumAggregation<uint64_t>::create());

    class MockedExecutablePipeline : public ExecutablePipeline {
      public:
        uint32_t execute(TupleBuffer&, void*, WindowManagerPtr, QueryExecutionContextPtr, WorkerContextRef) override {
            return 0;
        }
    };

    class MockedPipelineExecutionContext : public PipelineExecutionContext {
      public:
        MockedPipelineExecutionContext() : PipelineExecutionContext(0, nullptr, [](TupleBuffer&, WorkerContextRef) {
                                           }) {
            // nop
        }
    };
    auto executable = std::make_shared<MockedExecutablePipeline>();
    auto context = std::make_shared<MockedPipelineExecutionContext>();
    auto nextPipeline = PipelineStage::create(0, 1, executable, context, nullptr);
    w->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager(), nextPipeline, 0);

    auto windowState = (StateVariable<int64_t, WindowSliceStore<int64_t>*>*) w->getWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    store->setLastWatermark(7);

    ts = 14;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);

    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
    auto typedWindowHandler = w->as<int64_t, int64_t, int64_t, int64_t>();
    typedWindowHandler->aggregateWindows(10, store, windowDef, buf);
    typedWindowHandler->aggregateWindows(10, store, windowDef, buf);

    size_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3] << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerSlicingWindow) {
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);

    auto aggregation = SumAggregationDescriptor::on(Attribute("id"));

    auto windowDef = std::make_shared<LogicalWindowDefinition>(
        LogicalWindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("value")), Milliseconds(10)), DistributionCharacteristic::createSlicingWindowType()));

    auto w = WindowHandlerFactory::create<int64_t, int64_t, int64_t, int64_t>(windowDef, ExecutableSumAggregation<int64_t>::create());

    class MockedExecutablePipeline : public ExecutablePipeline {
      public:
        uint32_t execute(TupleBuffer&, void*, WindowManagerPtr, QueryExecutionContextPtr, WorkerContext&) override {
            return 0;
        }
    };

    class MockedPipelineExecutionContext : public PipelineExecutionContext {
      public:
        MockedPipelineExecutionContext() : PipelineExecutionContext(0, nullptr, [](TupleBuffer&, WorkerContext&) {
                                           }) {
            // nop
        }
    };
    auto executable = std::make_shared<MockedExecutablePipeline>();
    auto context = std::make_shared<MockedPipelineExecutionContext>();
    auto nextPipeline = PipelineStage::create(0, 1, executable, context, nullptr);
    w->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager(), nextPipeline, 0);

    auto windowState = (StateVariable<int64_t, WindowSliceStore<int64_t>*>*) w->getWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    store->setLastWatermark(7);

    ts = 14;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);

    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
    auto typedWindowHandler = w->as<int64_t, int64_t, int64_t, int64_t>();
    typedWindowHandler->aggregateWindows(10, store, windowDef, buf);
    typedWindowHandler->aggregateWindows(11, store, windowDef, buf);//this call should not change anything

    ASSERT_NE(buf.getBuffer(), nullptr);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3] << std::endl;

    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

TEST_F(WindowManagerTest, testWindowTriggerCombiningWindow) {
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, conf);

    auto aggregation = SumAggregationDescriptor::on(Attribute("id"));

    auto windowDef = std::make_shared<LogicalWindowDefinition>(
        LogicalWindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("value")), Milliseconds(10)), DistributionCharacteristic::createCombiningWindowType()));

    auto w = WindowHandlerFactory::create<int64_t, int64_t, int64_t, int64_t>(windowDef, ExecutableSumAggregation<int64_t>::create());

    class MockedExecutablePipeline : public ExecutablePipeline {
      public:
        uint32_t execute(TupleBuffer&, void*, WindowManagerPtr, QueryExecutionContextPtr, WorkerContext&) override {
            return 0;
        }
    };

    class MockedPipelineExecutionContext : public PipelineExecutionContext {
      public:
        MockedPipelineExecutionContext() : PipelineExecutionContext(0, nullptr, [](TupleBuffer&, WorkerContextRef) {
                                           }) {
            // nop
        }
    };
    auto executable = std::make_shared<MockedExecutablePipeline>();
    auto context = std::make_shared<MockedPipelineExecutionContext>();
    auto nextPipeline = PipelineStage::create(0, 1, executable, context, nullptr);
    w->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager(), nextPipeline, 0);

    auto windowState = (StateVariable<int64_t, WindowSliceStore<int64_t>*>*) w->getWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    store->setLastWatermark(7);

    ts = 14;
    w->updateAllMaxTs(ts, 0);
    w->getWindowManager()->sliceStream(ts, store);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;

    ASSERT_EQ(aggregates[sliceIndex], 1);

    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();

    auto typedWindowHandler = w->as<int64_t, int64_t, int64_t, int64_t>();
    typedWindowHandler->aggregateWindows(10, store, windowDef, buf);
    typedWindowHandler->aggregateWindows(11, store, windowDef, buf);

    size_t tupleCnt = buf.getNumberOfTuples();

    ASSERT_NE(buf.getBuffer(), nullptr);
    ASSERT_EQ(tupleCnt, 1);

    uint64_t* tuples = (uint64_t*) buf.getBuffer();
    std::cout << "tuples[0]=" << tuples[0] << " tuples[1=" << tuples[1] << " tuples[2=" << tuples[2] << " tuples[3=" << tuples[3] << std::endl;
    ASSERT_EQ(tuples[0], 0);
    ASSERT_EQ(tuples[1], 10);
    ASSERT_EQ(tuples[2], 10);
    ASSERT_EQ(tuples[3], 1);
}

}// namespace NES
