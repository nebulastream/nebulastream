#include "gtest/gtest.h"

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <future>
#include <iostream>
#include <QueryCompiler/GeneratedQueryExecutionPlanBuilder.hpp>
#include <utility>

#include "../util/DummySink.hpp"
#include "../util/SchemaSourceDescriptor.hpp"
#include "../util/TestQuery.hpp"
#include <Catalogs/StreamCatalog.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
using namespace NES;

class QueryExecutionTest : public testing::Test {
  public:
    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryExecutionTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup QueryCatalogTest test class.");
        // create test input buffer
        testSchema = Schema::create()
                         ->addField("id", BasicType::INT64)
                         ->addField("one", BasicType::INT64)
                         ->addField("value", BasicType::INT64);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_DEBUG("Tear down QueryExecutionTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_DEBUG("Tear down QueryExecutionTest test class.");
    }

    SchemaPtr testSchema;
};

/**
 * @brief A window source, which generates data consisting of (key, value, ts).
 * Key = 1||2
 * Value = 1
 * Ts = #Iteration
 */
class WindowSource : public NES::DefaultSource {
  public:
    int64_t timestamp = 1;
    WindowSource(SchemaPtr schema,
                 BufferManagerPtr bufferManager,
                 QueryManagerPtr queryManager,
                 const uint64_t numbersOfBufferToProduce,
                 size_t frequency) : DefaultSource(std::move(schema), std::move(bufferManager), std::move(queryManager), numbersOfBufferToProduce, frequency) {}

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = bufferManager->getBufferBlocking();
        auto rowLayout = createRowLayout(schema);
        for (int i = 0; i < 10; i++) {
            rowLayout->getValueField<int64_t>(i, 0)->write(buffer, i % 2);
            rowLayout->getValueField<int64_t>(i, 1)->write(buffer, 1);
            rowLayout->getValueField<int64_t>(i, 2)->write(buffer, Seconds(timestamp).getTime());
        }
        buffer.setNumberOfTuples(10);
        timestamp = timestamp + 10;
        return buffer;
    };

    static DataSourcePtr create(BufferManagerPtr bufferManager,
                                QueryManagerPtr queryManager,
                                const uint64_t numbersOfBufferToProduce,
                                size_t frequency) {
        auto windowSchema = Schema::create()
                                ->addField("key", BasicType::INT64)
                                ->addField("value", BasicType::INT64)
                                ->addField("ts", BasicType::INT64);
        return std::make_shared<WindowSource>(windowSchema, bufferManager, queryManager, numbersOfBufferToProduce, frequency);
    }
};

typedef std::shared_ptr<DefaultSource> DefaultSourcePtr;

class TestSink : public SinkMedium {
  public:
    TestSink(uint64_t expectedBuffer, SchemaPtr schema, BufferManagerPtr bufferManager) :
        SinkMedium(std::make_shared<NesFormat>(schema, bufferManager)),
                                        expectedBuffer(expectedBuffer){};

    static std::shared_ptr<TestSink> create(uint64_t expectedBuffer, SchemaPtr schema, BufferManagerPtr bufferManager) {
        return std::make_shared<TestSink>(expectedBuffer, schema, bufferManager);
    }

    bool writeData(TupleBuffer& input_buffer) override {
        std::unique_lock lock(m);
        NES_DEBUG("TestSink: got buffer " << input_buffer);
        NES_DEBUG(UtilityFunctions::prettyPrintTupleBuffer(input_buffer, getSchemaPtr()));
        resultBuffers.emplace_back(std::move(input_buffer));
        if (resultBuffers.size() == expectedBuffer) {
            completed.set_value(true);
        }
        return true;
    }

    /**
     * @brief Factory to create a new TestSink.
     * @param expectedBuffer number of buffers expected this sink should receive.
     * @return
     */


    TupleBuffer& get(size_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override{};

    std::string toString()
    {
        return "Test_Sink";
    }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~TestSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };


    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

    SinkMediumTypes getSinkMediumType()
    {
        return SinkMediumTypes::PRINT_SINK;
    }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }
    std::vector<TupleBuffer> resultBuffers;
    std::mutex m;
    uint64_t expectedBuffer;

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

    NodeEnginePtr nodeEngine = NodeEngine::create("127.0.0.1", 31337);


    // creating query plan
    auto testSource = createDefaultDataSourceWithSchemaForOneBuffer(testSchema,
                                                                    nodeEngine->getBufferManager(),
                                                                    nodeEngine->getQueryManager());


    auto query = TestQuery::from(testSource->getSchema())
                     .filter(Attribute("id") < 5)
                     .sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase =  TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0], nodeEngine);
    auto testSink = std::make_shared<TestSink>(10, testSchema, nodeEngine->getBufferManager());



    auto plan = GeneratedQueryExecutionPlanBuilder::create()
                    .addSink(testSink)
                    .addSource(testSource)
                    .addOperatorQueryPlan(generatableOperators)
                    .setCompiler(nodeEngine->getCompiler())
                    .setBufferManager(nodeEngine->getBufferManager())
                    .setQueryManager(nodeEngine->getQueryManager())
                    .setQueryId(1)
                    .build();

    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), QueryExecutionPlan::Created);
    EXPECT_EQ(plan->numberOfPipelineStages(), 1);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto memoryLayout = createRowLayout(testSchema);
    fillBuffer(buffer, memoryLayout);
    plan->setup();
    ASSERT_EQ(plan->getStatus(), QueryExecutionPlan::Deployed);
    plan->start();
    ASSERT_EQ(plan->getStatus(), QueryExecutionPlan::Running);
    plan->getStage(0)->execute(buffer);

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

/**
 * @brief This tests creates a windowed query.
 * WindowSource -> windowOperator -> windowScan -> TestSink
 */
TEST_F(QueryExecutionTest, windowQuery) {

    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337);

    // Create Operator Tree
    // 1. add window source and create two buffers each second one.
    auto windowSource = WindowSource::create(
        nodeEngine->getBufferManager(),
        nodeEngine->getQueryManager(), /*bufferCnt*/ 2, /*frequency*/ 1);


    auto query = TestQuery::from(windowSource->getSchema());
    // 2. dd window operator:
    // 2.1 add Tumbling window of size 10s and a sum aggregation on the value.
    auto windowType = TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("ts")), Seconds(10));
    auto aggregation = Sum::on(Attribute("value"));
    query = query.windowByKey(Attribute("key"), windowType, aggregation);

    // 3. add sink. We expect that this sink will receive one buffer
    auto windowResultSchema = Schema::create()->addField("sum", BasicType::INT64);
    auto testSink = TestSink::create(/*expected result buffer*/ 1, windowResultSchema, nodeEngine->getBufferManager());
    query.sink(DummySink::create());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());
    auto translatePhase =  TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0], nodeEngine);

    auto builder = GeneratedQueryExecutionPlanBuilder::create()
                       .setQueryManager(nodeEngine->getQueryManager())
                       .setBufferManager(nodeEngine->getBufferManager())
                       .setCompiler(nodeEngine->getCompiler())
                       .setQueryId(1)
                       .addSource(windowSource)
                       .addSink(testSink)
                       .addOperatorQueryPlan(generatableOperators);

    nodeEngine->registerQueryInNodeEngine(builder.build());
    nodeEngine->startQuery(1);

    // wait till all buffers have been produced
    testSink->completed.get_future().get();

    // get result buffer, which should contain two results.
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);
    auto& resultBuffer = testSink->get(0);
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 2);
    auto resultLayout = createRowLayout(windowResultSchema);
    for (int recordIndex = 0; recordIndex < 2; recordIndex++) {
        auto windowAggregationValue = resultLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer);
        EXPECT_EQ(windowAggregationValue, 5);
    }
    nodeEngine->stopQuery(1);
}

// P1 = Source1 -> filter1
// P2 = Source2 -> filter2
// P3 = [P1|P2] -> merge -> SINK
// So, merge is a blocking window_scan with two children.
TEST_F(QueryExecutionTest, mergeQuery) {

    NodeEnginePtr nodeEngine = NodeEngine::create("127.0.0.1", 31337);

    auto testSource1 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager());

    auto query1 = TestQuery::from(testSource1->getSchema());

    query1 = query1.filter(Attribute("id") < 5);

    // creating P2
    auto testSource2 = createDefaultDataSourceWithSchemaForOneBuffer(testSchema, nodeEngine->getBufferManager(),
                                                                     nodeEngine->getQueryManager());
    auto query2 = TestQuery::from(testSource2->getSchema()).filter(Attribute("id") <= 5);


    // creating P3
    // merge does not change schema
    SchemaPtr ptr = std::make_shared<Schema>(testSchema);
    auto mergedQuery = query2.merge(&query1).sink(DummySink::create());

    auto testSink = std::make_shared<TestSink>(10, testSchema, nodeEngine->getBufferManager());

    auto typeInferencePhase = TypeInferencePhase::create(nullptr);
    auto queryPlan = typeInferencePhase->execute(mergedQuery.getQueryPlan());
    auto translatePhase =  TranslateToGeneratableOperatorPhase::create();
    auto generatableOperators = translatePhase->transform(queryPlan->getRootOperators()[0], nodeEngine);


    auto builder = GeneratedQueryExecutionPlanBuilder::create()
        .setQueryManager(nodeEngine->getQueryManager())
        .setBufferManager(nodeEngine->getBufferManager())
        .setCompiler(nodeEngine->getCompiler())
        .addOperatorQueryPlan(generatableOperators)
        .setQueryId(1)
        .addSource(testSource1)
        .addSource(testSource2)
        .addSink(testSink);

    auto plan = builder.build();
    nodeEngine->getQueryManager()->registerQuery(plan);

    // The plan should have three pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 3);

    // TODO switch to event time if that is ready to remove sleep
    auto memoryLayout = createRowLayout(testSchema);
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    fillBuffer(buffer, memoryLayout);
    // TODO do not rely on sleeps
    // ingest test data
    plan->setup();
    plan->start();
    auto stage_0 = plan->getStage(0);
    auto stage_1 = plan->getStage(1);
    for (int i = 0; i < 10; i++) {

        stage_0->execute(buffer);// P1
        stage_1->execute(buffer);// P2
        // Contfext -> Context 1 and Context 2;
        //
        // P1 -> P2 -> P3
        // P1 -> 10 tuples -> sum=10;
        // P2 -> 10 tuples -> sum=10;
        // P1 -> 10 tuples -> P2 -> sum =10;
        // P2 -> 20 tuples -> sum=20;
        // TODO why sleep here?
        sleep(1);
    }
    testSink->completed.get_future().get();
    plan->stop();

    auto& resultBuffer = testSink->get(0);
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5);// how to interpret this?

    for (int recordIndex = 0; recordIndex < 5; recordIndex++) {
        EXPECT_EQ(
            memoryLayout->getValueField<int64_t>(recordIndex, /*fieldIndex*/ 0)->read(resultBuffer),
            recordIndex);
    }

    testSink->shutdown();
    testSource1->stop();
    testSource2->stop();
}