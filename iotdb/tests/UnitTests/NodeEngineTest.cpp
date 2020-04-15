#include <cassert>
#include <iostream>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Util/Logger.hpp>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <sstream>
#include <future>
using namespace std;

#define DEBUG_OUTPUT
namespace NES {

std::string expectedOutput =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------+";

std::string joinedExpectedOutput =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------+";

std::string joinedExpectedOutput10 =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|20|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|30|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|40|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|50|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|60|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|70|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|80|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|90|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|100|\n"
    "+----------------------------------------------------+";

std::string filePath = "file.txt";

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    std::atomic<uint64_t> count;
    std::atomic<uint64_t> sum;
    bool longRunning;
    std::promise<bool> completedPromise;
    CompiledTestQueryExecutionPlan(bool longRunning = false)
        :
        HandCodedQueryExecutionPlan(),
        count(0),
        sum(0),
        longRunning(longRunning) {
    }

    bool firstPipelineStage(const TupleBuffer&) {
        return false;
    }

    bool executeStage(uint32_t pipeline_stage_id, TupleBuffer& inBuf) {
        auto tuples = inBuf.getBufferAs<uint64_t>();

        NES_INFO("Test: Start execution");

        for (size_t i = 0; i < inBuf.getNumberOfTuples(); ++i) {
            count++;
            sum += tuples[i];
        }

        NES_INFO(
            "Test: query result = Processed Block:" << inBuf.getNumberOfTuples() << " count: " << count << "sum: "
                                                    << sum)

        DataSinkPtr sink = this->getSinks()[0];
        NES_DEBUG("TEST: try to get buffer")
        //  sink->getSchema().getSchemaSize();
        auto outputBuffer = BufferManager::instance().getBufferBlocking();
        NES_DEBUG("TEST: got buffer")
        u_int32_t* arr = (u_int32_t*) outputBuffer.getBuffer();
        arr[0] = sum;
        outputBuffer.setNumberOfTuples(1);
        outputBuffer.setTupleSizeInBytes(4);
        //  ysbRecordResult* outputBufferPtr = (ysbRecordResult*)outputBuffer->buffer;
        sink->writeData(outputBuffer);
        if (!longRunning) {
            completedPromise.set_value(true);
        } else {
            if (sum == 100) {
                completedPromise.set_value(true);
            }
        }
        return true;
    }
};
typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

/**
 * @brief test for the engine
 * TODO: add more test cases
 *  - More complex queries
 *  - concurrent queries
 *  - long running queries
 *
 */
class EngineTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("EngineTest.log", NES::LOG_DEBUG);
        remove(filePath.c_str());
        BufferManager::instance().configure(1024, 1024);
        NES_INFO("Setup EngineTest test class.");
    }
    static void TearDownTestCase() {
        remove(filePath.c_str());
        remove("qep1.txt");
        remove("qep2.txt");
        BufferManager::instance().requestShutdown();
        std::cout << "Tear down EngineTest class." << std::endl;
    }
};
/**
 * Helper Methods
 */
void testOutput() {
    ifstream testFile(filePath.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(filePath.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(filePath.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(std::string path) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

void testOutput(std::string path, std::string expectedOutput) {
    ifstream testFile(path.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    EXPECT_TRUE(response == 0);
}

CompiledTestQueryExecutionPlanPtr setupQEP() {
    CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
    SchemaPtr sch = Schema::create()
        ->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createBinaryFileSinkWithSchema(sch, filePath);
    qep->addDataSource(source);
    qep->addDataSink(sink);
    return qep;
}

/**
 * Test methods
 */
TEST_F(EngineTest, start_stop_engine_empty) {
    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->stop();
    delete ptr;
}

TEST_F(EngineTest, deploy_start_stop_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    std::cout << "Query " << qep <<
    endl;

    auto ptr = new NodeEngine();
    ptr->deployQuery(qep);
    ptr->start();
    qep->completedPromise.get_future().get();
    ptr->stop();
    testOutput();
    delete ptr;
}

TEST_F(EngineTest, start_deploy_stop_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep);
    qep->completedPromise.get_future().get();
    ptr->stop();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, start_deploy_undeploy_stop_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep);
    qep->completedPromise.get_future().get();
    ptr->undeployQuery(qep);
    ptr->stop();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, startWithRedeploy_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep);
    qep->completedPromise.get_future().get();
    ptr->stop();
    sleep(1);
    std::cout << "Starting Query again: " << endl;
    ptr->startWithRedeploy();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, stopWithRedeploy_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep);
    sleep(1);
    ptr->stopWithUndeploy();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, resetQEP_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep);
    sleep(1);
    ptr->resetQEPs();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, change_dop_with_restart_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->setDOPWithRestart(2);
    ptr->start();
    ptr->deployQuery(qep);
    sleep(1);
    ptr->stop();

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, change_dop_without_restart_test) {
    CompiledTestQueryExecutionPlanPtr qep = setupQEP();

    NodeEngine* ptr = new NodeEngine();
    ptr->setDOPWithoutRestart(2);
    ptr->start();
    ptr->deployQuery(qep);
    sleep(1);
    ptr->stop();
    ptr->start();
    EXPECT_EQ(ptr->getDOP(), size_t(2));

    testOutput();
    delete ptr;
}

TEST_F(EngineTest, DISABLED_parallel_different_source_test) {
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source2 =
  createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
  SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
  DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch2, "qep2.txt");
  qep2->addDataSource(source2);
  qep2->addDataSink(sink2);

    NodeEngine* ptr = new NodeEngine();
    ptr->start();
    ptr->deployQuery(qep1);
    sleep(1);
    ptr->deployQuery(qep2);
    sleep(2);
    ptr->stop();

    testOutput("qep1.txt");
    testOutput("qep2.txt");
    delete ptr;
}

TEST_F(EngineTest, DISABLED_parallel_same_source_test) {
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

    CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
    DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch1, "qep2.txt");
    qep2->addDataSource(source1);
    qep2->addDataSink(sink2);

    NodeEngine* ptr = new NodeEngine();
    ptr->deployQueryWithoutStart(qep1);
    ptr->deployQueryWithoutStart(qep2);
    ptr->start();
    source1->start();

    sleep(2);
    ptr->stop();

    testOutput("qep1.txt");
    testOutput("qep2.txt");

    delete ptr;
}

TEST_F(EngineTest, DISABLED_parallel_same_sink_test) {
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep12.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source2 =
  createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
  SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
  qep2->addDataSource(source1);
  qep2->addDataSink(sink1);

    NodeEngine* ptr = new NodeEngine();
    ptr->deployQueryWithoutStart(qep1);
    ptr->deployQueryWithoutStart(qep2);
    ptr->start();
    source1->start();

    sleep(1);
    ptr->stop();
    testOutput("qep12.txt", joinedExpectedOutput);
    delete ptr;
}

TEST_F(EngineTest, DISABLED_parallel_same_source_and_sink_test) {
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep3.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

    CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
    qep2->addDataSource(source1);
    qep2->addDataSink(sink1);

    NodeEngine* ptr = new NodeEngine();
    ptr->deployQueryWithoutStart(qep1);
    ptr->deployQueryWithoutStart(qep2);
    ptr->start();
    source1->start();

    sleep(1);
    ptr->stop();

    testOutput("qep3.txt", joinedExpectedOutput);
    delete ptr;
}
//TODO: enable after buffer redesign
TEST_F(EngineTest, blocking_test) {
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForVarBuffers(10, 0);
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep12.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);

    NodeEngine* ptr = new NodeEngine();
    ptr->deployQueryWithoutStart(qep1);
    ptr->start();
    source1->start();
    qep1->completedPromise.get_future().get();
    source1->stop();
    ptr->stop();
    testOutput("qep12.txt", joinedExpectedOutput10);
    delete ptr;
}

}
