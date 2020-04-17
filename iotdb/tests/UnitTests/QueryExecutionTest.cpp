#include "gtest/gtest.h"

#include <iostream>

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
        std::cout << "Setup QueryCatalogTest test class." << std::endl;
        NES::Dispatcher::instance();
        NES::BufferManager::instance().configure(4096, 1024);
        NES::ThreadPool::instance();
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);

        std::cout << "Setup QueryCatalogTest test case." << std::endl;

        ThreadPool::instance().setNumberOfThreadsWithRestart(1);
        ThreadPool::instance().start();

    // create test input buffer
    testSchema = Schema::create()
        ->addField(createField("id", BasicType::INT64))
        ->addField(createField("one", BasicType::INT64))
        ->addField(createField("value", BasicType::INT64));
    TupleBuffer testInputBuffer = BufferManager::instance().getBufferBlocking();
    memoryLayout = createRowLayout(testSchema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->write(
          testInputBuffer, recordIndex);
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/1)->write(
          testInputBuffer, 1);
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/2)->write(
          testInputBuffer, recordIndex % 2);
    }
    testInputBuffer.setNumberOfTuples(10);
    testInputBuffer.setTupleSizeInBytes(testSchema->getSchemaSizeInBytes());
  }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down QueryCatalogTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down QueryCatalogTest test class." << std::endl;
    }
//    TupleBuffer testInputBuffer;
    SchemaPtr testSchema;
    MemoryLayoutPtr memoryLayout;
};

class TestSink : public DataSink {
  public:

    bool writeData(TupleBuffer& input_buffer) override {
        NES_DEBUG("TestSink: got buffer " << input_buffer);
        NES_DEBUG(NES::toString(input_buffer, this->getSchema()));
        resultBuffers.push_back(input_buffer);
        return true;
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override {
    };
    void shutdown() override {
        for (auto b: resultBuffers) {
            b.release();
        }
    };
    ~TestSink() override {
    };
    SinkType getType() const override {
        return SinkType::PRINT_SINK;
    }

    uint32_t getNumberOfResultBuffers() {
        return resultBuffers.size();
    }

    std::vector<TupleBuffer> resultBuffers;
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
    fillBuffer(buffer, memoryLayout);
    plan->executeStage(0, buffer);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto resultBuffer = testSink->resultBuffers[0];
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(
            memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(
                buffer),
            recordIndex);
    }
}
TEST_F(QueryExecutionTest, windowQuery) {

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
    Dispatcher::instance().registerQueryWithoutStart(plan);
    plan->setup();
    plan->start();

    // The plan should have one pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 2);
    // TODO switch to event time if that is ready to remove sleep
    auto buffer = BufferManager::instance().getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // ingest test data
    for (int i = 0; i < 10; i++) {
        plan->executeStage(0, buffer);
        sleep(1);
    }
    plan->stop();
    sleep(1);

  auto resultBuffer = testSink->resultBuffers[2];
  // The output buffer should contain 5 tuple;
  EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
  auto resultLayout = createRowLayout(ptr);
  for (int recordIndex = 0; recordIndex < 2; recordIndex++) {
    EXPECT_EQ(
        resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(
            resultBuffer),
        10);
  }
}

