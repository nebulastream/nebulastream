#include <map>
#include <vector>

#include <API/Window/WindowAggregation.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <NodeEngine/NodeEngine.hpp>

#include <QueryLib/WindowManagerLib.hpp>
#include <Util/Logger.hpp>
#include <cassert>
#include <cstdlib>
#include <gtest/gtest.h>
#include <iostream>
#include <log4cxx/appender.h>
#include <random>
#include <thread>

#include <API/Schema.hpp>
#include <QueryCompiler/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declarations/StructDeclaration.hpp>
#include <Windows/WindowHandler.hpp>

namespace NES {
class WindowManagerTest : public testing::Test {
  public:
    void SetUp()
    {
        NES::setupLogging("WindowManagerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WindowMangerTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down WindowManager test class." << std::endl;
    }

    const size_t buffers_managed = 10;
    const size_t buffer_size = 4 * 1024;
};

class TestAggregation : public WindowAggregation {
  public:
    TestAggregation() : WindowAggregation(){};
    void compileLiftCombine(CompoundStatementPtr currentCode, BinaryOperatorStatement partialRef,
                            StructDeclaration inputStruct, BinaryOperatorStatement inputRef){};
};

TEST_F(WindowManagerTest, testSumAggregation)
{
    auto field = AttributeField::create("test", DataTypeFactory::createInt64());
    const WindowAggregationPtr aggregation = Sum::on(Field(field));
    if (Sum* store = dynamic_cast<Sum*>(aggregation.get())) {
        auto partial = store->lift<int64_t, int64_t>(1L);
        auto partial2 = store->lift<int64_t, int64_t>(2L);
        auto combined = store->combine<int64_t>(partial, partial2);
        auto final = store->lower<int64_t, int64_t>(combined);
        ASSERT_EQ(final, 3);
    }
}

TEST_F(WindowManagerTest, testCheckSlice)
{
    auto store = new WindowSliceStore<int64_t>(0L);
    auto aggregation = std::make_shared<TestAggregation>(TestAggregation());

    auto windowDef = std::make_shared<WindowDefinition>(
        WindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(AttributeField::create("ts", DataTypeFactory::createUInt64())), Seconds(60))));

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

TEST_F(WindowManagerTest, testWindowTrigger) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);

    auto aggregation = Sum::on(schema->get("id"));

    auto windowDef = std::make_shared<WindowDefinition>(
        WindowDefinition(aggregation, TumblingWindow::of(TimeCharacteristic::createEventTime(AttributeField::create("test", DataTypeFactory::createUInt64())), Milliseconds(10))));

    auto w = WindowHandler(windowDef, nodeEngine->getQueryManager(), nodeEngine->getBufferManager());
    w.setup(nullptr, 0);

    auto windowState = (StateVariable<int64_t, WindowSliceStore<int64_t>*>*)w.getWindowState();
    auto keyRef = windowState->get(10);
    keyRef.valueOrDefault(0);
    auto store = keyRef.value();

    uint64_t ts = 7;
    w.getWindowManager()->sliceStream(ts, store);
    auto sliceIndex = store->getSliceIndexByTs(ts);
    auto& aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;

    ts = 14;
    w.getWindowManager()->sliceStream(ts, store);
    sliceIndex = store->getSliceIndexByTs(ts);
    aggregates = store->getPartialAggregates();
    aggregates[sliceIndex]++;
    std::cout << aggregates[sliceIndex] << std::endl;
    // ASSERT_EQ(buffers_count, buffers_managed);

    ASSERT_EQ(aggregates[sliceIndex], 1);

    auto buf = nodeEngine->getBufferManager()->getBufferBlocking();
    w.aggregateWindows<int64_t, int64_t>(store, windowDef, buf);

    size_t tupleCnt = buf.getNumberOfTuples();

    assert(buf.getBuffer() != NULL);
    assert(tupleCnt == 1);

    uint64_t* tuples = (uint64_t*)buf.getBuffer();
    ASSERT_EQ(tuples[0], 1);
}
} // namespace NES
