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
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeCharacteristic.hpp>
#include <Operators/LogicalOperators/Windows/Types/SlidingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <algorithm>
#include <atomic>
#include <gtest/gtest.h>
#include <iostream>
using namespace std;
namespace NES {

class KeyedEventTimeWindowHandlerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { NES_DEBUG("Setup KeyedEventTimeWindowHandlerTest test class."); }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Logger::setupLogging("KeyedEventTimeWindowHandlerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup KeyedEventTimeWindowHandlerTest test case.");
        size_t keySize = 8;
        size_t valueSize = 8;
        size_t nrEntries = 100;
        bufferManager = std::make_shared<Runtime::BufferManager>();
        hashMapFactory = std::make_shared<NES::Experimental::HashMapFactory>(bufferManager, keySize, valueSize, nrEntries);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_DEBUG("Tear down KeyedEventTimeWindowHandlerTest test case."); }

  public:
    std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory;
    Runtime::BufferManagerPtr bufferManager;
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::BufferManagerPtr bufferManager, uint64_t numberOfWorkerThreads)
        : PipelineExecutionContext(
            0,
            std::move(bufferManager),
            std::move(numberOfWorkerThreads),
            [](Runtime::TupleBuffer&, Runtime::WorkerContextRef) {
            },
            [](Runtime::TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>{}){
            // nop
        };
};

void processRecord(Windowing::Experimental::KeyedThreadLocalSliceStore& sliceStore, uint64_t ts, uint64_t key, uint64_t) {
    auto& slice = sliceStore.findSliceByTs(ts);
    auto& state = slice->getState();
    state.getEntry<uint64_t>(key);
}

TEST_F(KeyedEventTimeWindowHandlerTest, setupThreadLocalState) {
    auto tumblingWindowSize = 100;
    //auto windowDefinition = Windowing::LogicalWindowDefinition::create()
    auto windowType =
        Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(100));
    auto windowHandler = Windowing::Experimental::KeyedEventTimeWindowHandler(windowType, {0});
    auto pctx = MockedPipelineExecutionContext(bufferManager, 1);
    windowHandler.setup(pctx, hashMapFactory);
    ASSERT_NO_THROW(windowHandler.getThreadLocalSliceStore(0));
    ASSERT_ANY_THROW(windowHandler.getThreadLocalSliceStore(1));
}

TEST_F(KeyedEventTimeWindowHandlerTest, triggerThreadLocalState) {
    auto tumblingWindowSize = 100;
    //auto windowDefinition = Windowing::LogicalWindowDefinition::create()
    auto windowType =
        Windowing::TumblingWindow::of(Windowing::TimeCharacteristic::createIngestionTime(), Windowing::TimeMeasure(100));
    auto windowHandler = Windowing::Experimental::KeyedEventTimeWindowHandler(windowType, {0});
    auto pctx = MockedPipelineExecutionContext(bufferManager, 1);
    windowHandler.setup(pctx, hashMapFactory);
    auto threadLocalSliceStore = windowHandler.getThreadLocalSliceStore(0);
    for (size_t i = 0; i < 1000; i++) {
        processRecord(threadLocalSliceStore, i, i % 10, 42);
    }
    ASSERT_EQ(threadLocalSliceStore.getNumberOfSlices(), 10);
}

}// namespace NES
