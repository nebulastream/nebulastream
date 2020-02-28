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
        setupLogging();
        std::cout << "Setup QueryCatalogTest test case." << std::endl;

        // create test input buffer
        testSchema = Schema().create().addField(createField("id", BasicType::UINT8));
        testInputBuffer = BufferManager::instance().getBuffer();
        memoryLayout = createRowLayout(std::make_shared<Schema>(testSchema));
        for (int i = 0; i < 10; i++) {
            memoryLayout->writeField<uint8_t>(testInputBuffer, i, 0, i);
        }
        testInputBuffer->setNumberOfTuples(10);

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
    Schema testSchema;
    MemoryLayoutPtr memoryLayout;
  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "QueryCatalogTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NES::NESLogger->setLevel(log4cxx::Level::getDebug());
        //    logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NES::NESLogger->addAppender(file);
        NES::NESLogger->addAppender(console);
    }
};

class TestSink : public DataSink {
  public:

    bool writeData(const TupleBufferPtr input_buffer) override {
        resultBuffers.push_back(input_buffer);
        return true;
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override {};
    void shutdown() override {};
    ~TestSink() override {};
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
    auto source = createSourceOperator(createDefaultDataSourceWithSchemaForOneBuffer(testSchema));
    auto filter = createFilterOperator(createPredicate(Field(testSchema.get("id")) < 5));
    auto testSink = std::make_shared<TestSink>();
    auto sink = createSinkOperator(testSink);

    filter->addChild(source);
    source->setParent(filter);
    sink->addChild(filter);
    filter->setParent(sink);

    auto compiler = createDefaultQueryCompiler();
    auto plan = compiler->compile(sink);
    plan->addDataSink(testSink);

    // The plan should have one pipeline
    EXPECT_EQ(plan->numberOfPipelineStages(), 1);

    plan->executeStage(0, testInputBuffer);

    // This plan should produce one output buffer
    EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1);

    auto resultBuffer = testSink->resultBuffers[0];
    // The output buffer should contain 5 tuple;
    EXPECT_EQ(resultBuffer->getNumberOfTuples(), 5);

    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(memoryLayout->readField<uint8_t>(resultBuffer, i, 0), i);
    }
}



