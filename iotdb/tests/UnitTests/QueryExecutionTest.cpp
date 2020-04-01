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
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);

    std::cout << "Setup QueryCatalogTest test case." << std::endl;
    NES::Dispatcher::instance();
    NES::BufferManager::instance();
    NES::ThreadPool::instance();
    ThreadPool::instance().setNumberOfThreadsWithRestart(1);
    ThreadPool::instance().start();

    // create test input buffer
    testSchema = SchemaTemp::create()->addField(createField("id", BasicType::INT64))->addField(
        createField("one", BasicType::INT64))->addField(
        createField("value", BasicType::INT64));
    testInputBuffer = BufferManager::instance().getFixedSizeBuffer();
    memoryLayout = createRowLayout(testSchema);
    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->write(
          testInputBuffer, recordIndex);
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/1)->write(
          testInputBuffer, 1);
      memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/2)->write(
          testInputBuffer, recordIndex % 2);
    }
    testInputBuffer->setNumberOfTuples(10);
    testInputBuffer->setTupleSizeInBytes(testSchema.getSchemaSize());
  }

  /* Will be called before a test is executed. */
  void TearDown() {
    std::cout << "Tear down QueryCatalogTest test case." << std::endl;
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() {
    std::cout << "Tear down QueryCatalogTest test class." << std::endl;
  }
  TupleBufferPtr testInputBuffer;
  SchemaPtr testSchema;
  MemoryLayoutPtr memoryLayout;
};

class TestSink : public DataSink {
 public:

  bool writeData(const TupleBufferPtr input_buffer) override {
    NES_DEBUG("TestSink: got buffer " << input_buffer);
    NES_DEBUG(NES::toString(input_buffer.get(), this->getSchema()));
    input_buffer->print();
    char* buffer = new char[input_buffer->getBufferSizeInBytes()];
    TupleBufferPtr storePtr = std::make_shared<TupleBuffer>(buffer,
                                                            input_buffer->getBufferSizeInBytes(),
                                                        /**tupleSizeBytes*/0, /**numTuples*/
                                                        0);
    storePtr->copyInto(input_buffer);
    resultBuffers.push_back(storePtr);
    return true;
  }

  const std::string toString() const override {
    return "";
  }

  void setup() override {
  }
  ;
  void shutdown() override {
  }
  ;
  ~TestSink() override {
  }
  ;
  SinkType getType() const override {
    return SinkType::PRINT_SINK;
  }

  uint32_t getNumberOfResultBuffers() {
    return resultBuffers.size();
  }

  std::vector<TupleBufferPtr> resultBuffers;
};

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

  plan->executeStage(0, testInputBuffer);

  // This plan should produce one output buffer
  EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

  auto resultBuffer = testSink->resultBuffers[0];
  // The output buffer should contain 5 tuple;
  EXPECT_EQ(resultBuffer->getNumberOfTuples(), 5);

  for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
    EXPECT_EQ(
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(
            testInputBuffer),
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
  SchemaPtr resultSchema = SchemaTemp::create()->addField(
      createField("sum", BasicType::INT64));
  auto windowScan = createWindowScanOperator(resultSchema);
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
  // ingest test data
  for (int i = 0; i < 10; i++) {
    plan->executeStage(0, testInputBuffer);
    sleep(1);
  }
  plan->stop();
  sleep(1);

  auto resultBuffer = testSink->resultBuffers[2];
  // The output buffer should contain 5 tuple;
  EXPECT_EQ(resultBuffer->getNumberOfTuples(), 2);
  auto resultLayout = createRowLayout(resultSchema);
  for (int recordIndex = 0; recordIndex < 2; recordIndex++) {
    EXPECT_EQ(
        resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/0)->read(
            resultBuffer),
        10);
  }
}

