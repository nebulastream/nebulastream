#include "gtest/gtest.h"

#include <iostream>
#include <future>

#include <Catalogs/QueryCatalog.hpp>
#include <API/InputQuery.hpp>
#include <Services/CoordinatorService.hpp>

#include <Topology/NESTopologyManager.hpp>
#include <API/Schema.hpp>

#include <Util/Logger.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>

using namespace NES;

class QueryExecutionTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup QueryCatalogTest test class.");
        NES::BufferManager::instance().configure(4096, 1024);
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES_DEBUG("Setup QueryCatalogTest test case.");
        // create test input buffer
        testSchema = Schema::create()
            ->addField(createField("id", BasicType::INT64))
            ->addField(createField("one", BasicType::INT64))
            ->addField(createField("value", BasicType::INT64));
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_DEBUG("Tear down QueryCatalogTest test case.");
        Dispatcher::instance().resetDispatcher();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("Tear down QueryCatalogTest test class.");
        Dispatcher::instance().resetDispatcher();
    }

    SchemaPtr testSchema;
};

class TestSink : public DataSink {
  public:

    bool writeData(TupleBuffer& input_buffer) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink: got buffer " << input_buffer);
        NES_DEBUG(NES::toString(input_buffer, getSchema()));
        resultBuffers.emplace_back(std::move(input_buffer));
        if (resultBuffers.size() == 10) {
            completed.set_value(true);
        }
        return true;
    }

    TupleBuffer& get(size_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override {
    };

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~TestSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    SinkType getType() const override {
        return SinkType::PRINT_SINK;
    }

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }
  private:
    std::vector<TupleBuffer> resultBuffers;
    std::mutex m;
  public:
    std::promise<bool> completed;
};

void fillBuffer(TupleBuffer& buf, MemoryLayoutPtr memoryLayout) {
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->write(
            buf, recordIndex);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/1)->write(
            buf, 1);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/2)->write(
            buf, recordIndex%2);
    }
    buf.setNumberOfTuples(10);
}

TEST_F(QueryExecutionTest, filterQuery) {

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema);
    auto source = createSourceOperator(testSource);
    auto filter = createFilterOperator(
        createPredicate(Field(testSchema->get("id")) < 5));
    auto testSink = std::make_shared<TestSink>();
    auto sink = createSinkOperator(testSink);

    filter->addChild(source);
    source->setParent(filter);
    sink->addChild(filter);
    filter->setParent(sink);

    auto compiler = createDefaultQueryCompiler();
    auto plan = compiler->compile(sink);
    plan->addDataSink(testSink);
    plan->addDataSource(testSource);

    // The plan should have one pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 1);
    auto buffer = BufferManager::instance().getBufferBlocking();
    auto memoryLayout = createRowLayout(testSchema);
    fillBuffer(buffer, memoryLayout);
    plan->executeStage(0, buffer);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto& resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(
            memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(
                buffer),
            recordIndex);
    }
    testSink->shutdown();
    plan->stop();
}

TEST_F(QueryExecutionTest, windowQuery) {
    // TODO in this test, it is not clear what we are testing
    // TODO 10 windows are fired -> 10 output buffers in the sink
    // TODO however, we check the 2nd buffer only
    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema);
    auto source = createSourceOperator(testSource);
    auto aggregation = Sum::on(testSchema->get("one"));
    auto windowType = TumblingWindow::of(TimeCharacteristic::ProcessingTime,
                                         Milliseconds(2));
    auto windowOperator = createWindowOperator(
        createWindowDefinition(testSchema->get("value"), aggregation, windowType));
    Schema resultSchema = Schema().create()->addField(
        createField("sum", BasicType::INT64));
    SchemaPtr ptr = std::make_shared<Schema>(resultSchema);
    auto windowScan = createWindowScanOperator(ptr);
    auto testSink = std::make_shared<TestSink>();
    auto sink = createSinkOperator(testSink);

    windowOperator->addChild(source);
    source->setParent(windowOperator);
    windowScan->addChild(windowOperator);
    windowOperator->setParent(windowScan);
    sink->addChild(windowScan);
    windowScan->setParent(sink);

    auto compiler = createDefaultQueryCompiler();
    auto plan = compiler->compile(sink);
    plan->addDataSink(testSink);
    plan->addDataSource(testSource);
    Dispatcher::instance().registerQuery(plan);

    plan->setup();
    plan->start();

    // The plan should have one pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 2);
    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = createRowLayout(testSchema);
    auto buffer = BufferManager::instance().getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    for (int i = 0; i < 10; i++) {
        plan->executeStage(0, buffer);
        sleep(1);
    }
    testSink->completed.get_future().get();
    plan->stop();
    //    sleep(1);

    auto& resultBuffer = testSink->get(2); // TODO why the 2nd buffer?
    // The output buffer should contain 5 tuple;
    // TODO why do we check for 2 tuple?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
    auto resultLayout = createRowLayout(ptr);
    for (int recordIndex = 0; recordIndex < 2; recordIndex++) {
        auto v = resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(resultBuffer);
        EXPECT_EQ(v, 10);
    }
    testSink->shutdown();
    testSource->stop();
}

