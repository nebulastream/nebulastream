#include <cassert>
#include <iostream>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Util/Logger.hpp>
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

std::string testQueryId = "123";

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
    "+----------------------------https://app.diagrams.net/#G1uTDrT32L0Aep6CnvBZHbJJ1LsHDyZSZr------------------------+\n"
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
    std::promise<bool> completedPromise;
    CompiledTestQueryExecutionPlan()
        :
        HandCodedQueryExecutionPlan(),
        count(0),
        sum(0) {
    }

    bool firstPipelineStage(const TupleBuffer&) {
        return false;
    }

    bool executeStage(uint32_t pipeline_stage_id, TupleBuffer& inBuf) {
        auto tuples = inBuf.getBufferAs<uint64_t>();

        NES_INFO("Test: Start execution");

        uint64_t psum = 0;
        for (size_t i = 0; i < inBuf.getNumberOfTuples(); ++i) {
            psum += tuples[i];
        }
        count += inBuf.getNumberOfTuples();
        sum += psum;

        NES_INFO(
            "Test: query result = Processed Block:" << inBuf.getNumberOfTuples() << " count: " << count << " psum: "
                                                    << psum << " sum: " << sum)

        auto sink = getSinks()[0];
        NES_DEBUG("TEST: try to get buffer")
        TupleBuffer outputBuffer = inBuf;

        NES_DEBUG("TEST: got buffer")
        auto arr = outputBuffer.getBufferAs<uint32_t>();
        arr[0] = static_cast<uint32_t>(sum.load());
        outputBuffer.setNumberOfTuples(1);
        outputBuffer.setTupleSizeInBytes(4);
        NES_DEBUG("TEST: written " << arr[0]);
        sink->writeData(outputBuffer);

        if (sum == 10) {
            NES_DEBUG("TEST: result correct");
            completedPromise.set_value(true);
        } else {
            NES_DEBUG("TEST: result wrong ");
            completedPromise.set_value(false);
        }

        NES_DEBUG("TEST: return");
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
    void SetUp() {
        NES::setupLogging("EngineTest.log", NES::LOG_DEBUG);
        remove(filePath.c_str());
        NES_INFO("Setup EngineTest test class.");
    }
    static void TearDownTestCase() {
        remove(filePath.c_str());
        remove("qep1.txt");
        remove("qep2.txt");
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

bool checkCompleteOrTimeout(NodeEnginePtr ptr, std::string queryId, size_t expectedResult)
{
    size_t timeoutInSec = 3;
    size_t now = time(0);

    while(time(0) < now + timeoutInSec)
    {
        cout << "check result" << endl;
        if(ptr->getNumberOfProcessedBuffer(queryId) == expectedResult && ptr->getNumberOfProcessedTasks(queryId) == expectedResult)
        {
            cout << "results are correct" << endl;
            return true;
        }
    }

    cout << "expected results are not reached after timeout" << endl;
    return false;
}
CompiledTestQueryExecutionPlanPtr setupQEP(BufferManagerPtr bPtr, QueryManagerPtr dPtr, std::string queryId) {
    CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(bPtr, dPtr);
    SchemaPtr sch = Schema::create()
        ->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createBinaryFileSinkWithSchema(sch, filePath);
    qep->addDataSource(source);
    qep->addDataSink(sink);
    qep->setQueryId(queryId);
    return qep;
}

//TODO: add test for register and start only

/**
 * Test methods
 *     cout << "Stats=" << ptr->getStatistics() << endl;
 */
TEST_F(EngineTest, start_stop_engine_empty) { 
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->stop());
}

TEST_F(EngineTest, start_deploy_stop_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep = setupQEP(ptr->getBufferManager(), ptr->getQueryManager(), testQueryId);
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->deployQueryInNodeEngine(qep));
    qep->completedPromise.get_future().get();
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, testQueryId, 1));
    ASSERT_TRUE(!ptr->stop());

    testOutput();
}

TEST_F(EngineTest, start_deploy_undeploy_stop_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep = setupQEP(ptr->getBufferManager(), ptr->getQueryManager(), testQueryId);
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->deployQueryInNodeEngine(qep));
    qep->completedPromise.get_future().get();
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, testQueryId, 1));
    ASSERT_TRUE(ptr->undeployQuery(testQueryId));
    ASSERT_TRUE(ptr->stop());

    testOutput();
}

TEST_F(EngineTest, start_register_start_stop_deregister_stop_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep = setupQEP(ptr->getBufferManager(), ptr->getQueryManager(), testQueryId);
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep));
    ASSERT_TRUE(ptr->startQuery(testQueryId));
    qep->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, testQueryId, 1));

    ASSERT_TRUE(ptr->stopQuery(testQueryId));

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, testQueryId, 1));

    ASSERT_TRUE(ptr->unregisterQuery(testQueryId));
    ASSERT_TRUE(ptr->stop());

    testOutput();
}

TEST_F(EngineTest, parallel_different_source_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep1 = std::make_shared<CompiledTestQueryExecutionPlan>();
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(), ptr->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);
    qep1->setQueryId("1");

    CompiledTestQueryExecutionPlanPtr qep2 = std::make_shared<CompiledTestQueryExecutionPlan>();
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(),
                                                                                     ptr->getQueryManager());
    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch2, "qep2.txt");
    qep2->addDataSource(source2);
    qep2->addDataSink(sink2);
    qep2->setQueryId("2");

    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep1));
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep2));

    ASSERT_TRUE(ptr->startQuery("1"));
    ASSERT_TRUE(ptr->startQuery("2"));

    qep1->completedPromise.get_future().get();
    qep2->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "1", 1));
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "2", 1));

    ASSERT_TRUE(ptr->undeployQuery("1"));
    ASSERT_TRUE(ptr->undeployQuery("2"));

    ASSERT_TRUE(ptr->stop());

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}

TEST_F(EngineTest, parallel_same_source_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();

    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 = createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(),
                                                                                     ptr->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);
    qep1->setQueryId("1");

    CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
    DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch1, "qep2.txt");
    qep2->addDataSource(source1);
    qep2->addDataSink(sink2);
    qep2->setQueryId("2");


    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep1));
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep2));

    cout << "start qep1" << endl;
    ASSERT_TRUE(ptr->startQuery("1"));
    cout << "start qep2" << endl;
    ASSERT_TRUE(ptr->startQuery("2"));

    cout << "wait prom q1" << endl;
    qep1->completedPromise.get_future().get();

    cout << "wait prom q2" << endl;
    qep2->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "1", 1));
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "2", 1));

    cout << "undeploy qep1" << endl;
    ASSERT_TRUE(ptr->undeployQuery("1"));
    cout << "undeploy qep2" << endl;
    ASSERT_TRUE(ptr->undeployQuery("2"));

    cout << "stop" << endl;
    ASSERT_TRUE(ptr->stop());

    testOutput("qep1.txt");
    testOutput("qep2.txt");
}

TEST_F(EngineTest, parallel_same_sink_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(), ptr->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep12.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);
    qep1->setQueryId("1");


    CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source2 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(), ptr->getQueryManager());
    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    qep2->addDataSource(source2);
    qep2->addDataSink(sink1);
    qep2->setQueryId("2");


    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep1));
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep2));

    ASSERT_TRUE(ptr->startQuery("1"));
    ASSERT_TRUE(ptr->startQuery("2"));

    qep1->completedPromise.get_future().get();
    qep2->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "1", 1));
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "2", 1));

    ASSERT_TRUE(ptr->undeployQuery("1"));
    ASSERT_TRUE(ptr->undeployQuery("2"));

    ASSERT_TRUE(ptr->stop());
    testOutput("qep12.txt", joinedExpectedOutput);
}

TEST_F(EngineTest, parallel_same_source_and_sink_regstart_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
    DataSourcePtr source1 =
        createDefaultSourceWithoutSchemaForOneBufferForOneBuffer(ptr->getBufferManager(), ptr->getQueryManager());
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep3.txt");
    qep1->addDataSource(source1);
    qep1->addDataSink(sink1);
    qep1->setQueryId("1");

    CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
    qep2->addDataSource(source1);
    qep2->addDataSink(sink1);
    qep2->setQueryId("2");

    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep1));
    ASSERT_TRUE(ptr->registerQueryInNodeEngine(qep2));

    ASSERT_TRUE(ptr->startQuery("1"));
    ASSERT_TRUE(ptr->startQuery("2"));

    qep1->completedPromise.get_future().get();
    qep2->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "1", 1));
    ASSERT_TRUE(checkCompleteOrTimeout(ptr, "2", 1));

    ASSERT_TRUE(ptr->undeployQuery("1"));
    ASSERT_TRUE(ptr->undeployQuery("2"));

    ASSERT_TRUE(ptr->stop());

    testOutput("qep3.txt", joinedExpectedOutput);
}

TEST_F(EngineTest, start_stop_start_stop_test) {
    NodeEnginePtr ptr = std::make_shared<NodeEngine>();
    CompiledTestQueryExecutionPlanPtr qep = setupQEP(ptr->getBufferManager(), ptr->getQueryManager(), testQueryId);
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->deployQueryInNodeEngine(qep));
    qep->completedPromise.get_future().get();

    ASSERT_TRUE(checkCompleteOrTimeout(ptr, testQueryId, 1));

    ASSERT_TRUE(ptr->undeployQuery(testQueryId));
    ASSERT_TRUE(ptr->stop());
    ASSERT_TRUE(ptr->start());
    ASSERT_TRUE(ptr->stop());
    testOutput();
}
}

