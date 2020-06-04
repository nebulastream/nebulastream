#include "gtest/gtest.h"

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
using namespace NES;

class QueryExecutionTest : public testing::Test {
  public:
    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup QueryCatalogTest test class.");
        // create test input buffer
        testSchema = Schema::create()
                         ->addField(createField("id", BasicType::INT64))
                         ->addField(createField("one", BasicType::INT64))
                         ->addField(createField("value", BasicType::INT64));
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_DEBUG("Tear down QueryCatalogTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("Tear down QueryCatalogTest test class.");
    }

    SchemaPtr testSchema;
};

class TestSink : public DataSink {
  public:
    bool writeData(TupleBuffer& input_buffer) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink: got buffer " << input_buffer);
        NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchema()));
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

    void setup() override{};

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
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->write(buf, recordIndex);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 1)->write(buf, 1);
        memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 2)->write(buf, recordIndex % 2);
    }
    buf.setNumberOfTuples(10);
}

TEST_F(QueryExecutionTest, filterQuery) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                    nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager());
    auto source = createSourceOperator(testSource);
    auto filter = createFilterOperator(
        createPredicate(Field(testSchema->get("id")) < 5));
    auto testSink = std::make_shared<TestSink>();
    auto sink = createSinkOperator(testSink);

    filter->addChild(source);
    source->setParent(filter);
    sink->addChild(filter);
    filter->setParent(sink);

    auto compiler = createDefaultQueryCompiler(nodeEngine->getQueryManager());
    auto plan = compiler->compile(sink);
    plan->addDataSink(testSink);
    plan->addDataSource(testSource);
    plan->setBufferManager(nodeEngine->getBufferManager());
    plan->setQueryManager(nodeEngine->getQueryManager());

    // The plan should have one pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
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
            memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(buffer),
            recordIndex);
    }
    testSink->shutdown();
    plan->stop();
}

TEST_F(QueryExecutionTest, DISABLED_windowQuery) {
    // TODO in this test, it is not clear what we are testing
    // TODO 10 windows are fired -> 10 output buffers in the sink
    // TODO however, we check the 2nd buffer only
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                    nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager());
    auto source = createSourceOperator(testSource);
    auto aggregation = Sum::on(testSchema->get("one"));
    auto windowType = TumblingWindow::of(TimeCharacteristic::ProcessingTime,
                                         Milliseconds(2));
    auto windowOperator = createWindowOperator(
        createWindowDefinition(testSchema->get("value"), aggregation, windowType));
    Schema resultSchema = Schema::create()->addField(
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

    auto compiler = createDefaultQueryCompiler(nodeEngine->getQueryManager());
    compiler->setQueryManager(nodeEngine->getQueryManager());
    compiler->setBufferManager(nodeEngine->getBufferManager());
    auto plan = compiler->compile(sink);
    plan->addDataSink(testSink);
    plan->addDataSource(testSource);
    plan->setBufferManager(nodeEngine->getBufferManager());
    plan->setQueryManager(nodeEngine->getQueryManager());
    plan->setQueryId("1");

    nodeEngine->registerQueryInNodeEngine(plan);
    nodeEngine->startQuery("1");

    auto& resultBuffer = testSink->get(2);// TODO why the 2nd buffer?
    // The output buffer should contain 5 tuple;
    // TODO why do we check for 2 tuple?
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
    auto resultLayout = createRowLayout(ptr);
    for (int recordIndex = 0; recordIndex < 2; recordIndex++) {
        auto v = resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer);
        EXPECT_EQ(v, 10);
    }
    testSink->shutdown();
    testSource->stop();
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(QueryExecutionTest, mergeQuery) {

    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();
    // creating query plan
    // creating P1
    auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager());
    auto source1 = createSourceOperator(testSource1);
    auto filter1 = createFilterOperator(
        createPredicate(Field(testSchema->get("id")) < 5));

    filter1->addChild(source1);
    source1->setParent(filter1);

    // creating P2
    auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager());
    auto source2 = createSourceOperator(testSource2);
    auto filter2 = createFilterOperator(
        createPredicate(Field(testSchema->get("id")) < 5));
    filter2->addChild(source2);
    source2->setParent(filter2);

    // creating P3
    // merge does not change schema
    SchemaPtr ptr = std::make_shared<Schema>(testSchema);
    auto mergeOperator = createMergeOperator(ptr);
    mergeOperator->addChild(filter1);
    mergeOperator->addChild(filter2);
    filter1->setParent(mergeOperator);
    filter2->setParent(mergeOperator);
    auto windowScan = createWindowScanOperator(testSchema);
    mergeOperator->setParent(windowScan);
    windowScan->addChild(mergeOperator);

    auto testSink = std::make_shared<TestSink>();
    auto sink = createSinkOperator(testSink);

    windowScan->setParent(sink);
    sink->addChild(windowScan);

    //compile
    auto compiler = createDefaultQueryCompiler(
        nodeEngine->getQueryManager());
    compiler->setQueryManager(
        nodeEngine->getQueryManager());
    compiler->setBufferManager(nodeEngine->getBufferManager());
    auto plan = compiler->compile(sink);

    plan->addDataSink(testSink);
    plan->addDataSource(testSource1);
    plan->addDataSource(testSource2);
    plan->setBufferManager(nodeEngine->getBufferManager());
    plan->setQueryManager(nodeEngine->getQueryManager());
    nodeEngine->getQueryManager()->registerQuery(plan);
    plan->setup();
    plan->start();

    // The plan should have three pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = createRowLayout(testSchema);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    for (int i = 0; i < 10; i++) {

        plan->executeStage(0, buffer);// P1
        plan->executeStage(1, buffer);// P2
        // Context -> Context 1 and Context 2;
        //
        // P1 -> P2 -> P3
        // P1 -> 10 tuples -> sum=10;
        // P2 -> 10 tuples -> sum=10;
        // P1 -> 10 tuples -> P2 -> sum =10;
        // P2 -> 20 tuples -> sum=20;

        sleep(1);
    }
    testSink->completed.get_future().get();
    plan->stop();
    //    sleep(1);

    auto& resultBuffer = testSink->get(1);//1
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);
    auto resultLayout = createRowLayout(ptr);
    // Tony: two inputs are add up to P1 because of "chain pipeline". We need to rewrite the QEP.
    auto v = resultLayout->getValueField<int64_t>(0, /*fieldIndex*/ 0)->read(resultBuffer);
    EXPECT_EQ(v, 20);
    v = resultLayout->getValueField<int64_t>(1, /*fieldIndex*/ 0)->read(resultBuffer);
    EXPECT_EQ(v, 10);
    testSink->shutdown();
    testSource1->stop();
    testSource2->stop();
}