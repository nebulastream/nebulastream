#include <cassert>
#include <iostream>

#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <sstream>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

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
std::string filePath = "file.txt";

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
 public:
  uint64_t count;
  uint64_t sum;
  CompiledTestQueryExecutionPlan()
      :
      HandCodedQueryExecutionPlan(),
      count(0),
      sum(0) {
  }

  void setDataSource(DataSourcePtr source) {
    sources.push_back(source);
  }
  void setDataSink(DataSinkPtr sink) {
    sinks.push_back(sink);
  }
  bool firstPipelineStage(const TupleBuffer&) {
    return false;
  }

  bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) {
    uint64_t *tuples = (uint64_t*) buf->getBuffer();

    NES_INFO("Test: Start execution");

    for (size_t i = 0; i < buf->getNumberOfTuples(); ++i) {
      count++;
      sum += tuples[i];
    }

    NES_INFO(
        "Test: query result = Processed Block:" << buf->getNumberOfTuples() << " count: " << count << "sum: " << sum)
    assert(sum == 10);

    DataSinkPtr sink = this->getSinks()[0];
//  sink->getSchema().getSchemaSize();
    TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
    u_int32_t *arr = (u_int32_t*) outputBuffer->getBuffer();
    arr[0] = sum;
    outputBuffer->setNumberOfTuples(1);
    outputBuffer->setTupleSizeInBytes(4);
//  ysbRecordResult* outputBufferPtr = (ysbRecordResult*)outputBuffer->buffer;
    sink->writeData(outputBuffer);
    return true;
  }
};

typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

class MultiWorkerTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
    remove(filePath.c_str());
#endif
    NES_INFO("Setup EngineTest test class.");
  }
  static void TearDownTestCase() {
    remove(filePath.c_str());
    remove("qep1.txt");
    remove("qep2.txt");

    std::cout << "Tear down EngineTest class." << std::endl;
  }

 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout(
            "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "EngineTest.log");
    log4cxx::FileAppenderPtr file(
        new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(
        new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    NESLogger->setLevel(log4cxx::Level::getDebug());
//    logger->setLevel(log4cxx::Level::getInfo());

// add appenders and other will inherit the settings
    NESLogger->addAppender(file);
    NESLogger->addAppender(console);
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
  DataSourcePtr source = createTestSourceWithoutSchema();
  Schema sch = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink = createBinaryFileSinkWithSchema(sch, filePath);
  qep->setDataSource(source);
  qep->setDataSink(sink);
  return qep;
}

/**
 * Test methods
 */
TEST_F(MultiWorkerTest, start_stop_one_workers) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  cout << "coordinator started successfully" << endl;
  sleep(1);

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  wrk->start(/**blocking**/false, port);
  cout << "worker started successfully" << endl;

  sleep(5);
  cout << "wakeup" << endl;

  cout << "stopping worker" << endl;
  wrk->stop();

  cout << "stopping coordinator" << endl;
  crd->stopCoordinator();

}

TEST_F(MultiWorkerTest, start_stop_connect_workers) {
  cout << "start coordinator" << endl;
  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
  size_t port = crd->startCoordinator(/**blocking**/false);
  cout << "coordinator started successfully" << endl;

  cout << "start worker" << endl;
  NesWorkerPtr wrk = std::make_shared<NesWorker>();
  wrk->start(/**blocking**/false, port);
  cout << "worker started successfully" << endl;

  wrk->connect();
  cout << "worker started connected " << endl;

  sleep(2);
  cout << "stopping coordinator" << endl;
  crd->stopCoordinator();

  cout << "stopping worker" << endl;
  wrk->stop();
}

#if 0
TEST_F(EngineTest, deploy_start_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQuery(qep);
  ptr->start();
  sleep(1);
  ptr->stop();

  testOutput();
}

TEST_F(EngineTest, start_deploy_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();

  testOutput();
}

TEST_F(EngineTest, start_deploy_undeploy_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->undeployQuery(qep);
  ptr->stop();

  testOutput();
}

TEST_F(EngineTest, startWithRedeploy_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
  sleep(1);
  ptr->startWithRedeploy();

  testOutput();
}

TEST_F(EngineTest, stopWithRedeploy_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stopWithUndeploy();

  testOutput();
}

TEST_F(EngineTest, resetQEP_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->resetQEPs();

  testOutput();
}

TEST_F(EngineTest, change_dop_with_restart_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->setDOPWithRestart(2);
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();

  testOutput();
}

TEST_F(EngineTest, change_dop_without_restart_test) {
  CompiledTestQueryExecutionPlanPtr qep = setupQEP();

  NodeEngine *ptr = new NodeEngine();
  ptr->setDOPWithoutRestart(2);
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
  ptr->start();
  EXPECT_EQ(ptr->getDOP(), size_t(2));

  testOutput();
}

TEST_F(EngineTest, parallel_different_source_test) {
  CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source1 = createTestSourceWithoutSchema();
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
  qep1->setDataSource(source1);
  qep1->setDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source2 = createTestSourceWithoutSchema();
  Schema sch2 = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch2, "qep2.txt");
  qep2->setDataSource(source2);
  qep2->setDataSink(sink2);

  NodeEngine *ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep1);
  sleep(1);
  ptr->deployQuery(qep2);
  sleep(2);
  ptr->stop();

  testOutput("qep1.txt");
  testOutput("qep2.txt");
}

TEST_F(EngineTest, parallel_same_source_test) {
  CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source1 = createTestSourceWithoutSchema();
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep1.txt");
  qep1->setDataSource(source1);
  qep1->setDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  DataSinkPtr sink2 = createBinaryFileSinkWithSchema(sch1, "qep2.txt");
  qep2->setDataSource(source1);
  qep2->setDataSink(sink2);

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQueryWithoutStart(qep1);
  ptr->deployQueryWithoutStart(qep2);
  ptr->start();
  source1->start();

  sleep(1);
  ptr->stop();

  testOutput("qep1.txt");
  testOutput("qep2.txt");
}

TEST_F(EngineTest, parallel_same_sink_test) {
  CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source1 = createTestSourceWithoutSchema();
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep12.txt");
  qep1->setDataSource(source1);
  qep1->setDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source2 = createTestSourceWithoutSchema();
  Schema sch2 = Schema::create().addField("sum", BasicType::UINT32);
  qep2->setDataSource(source1);
  qep2->setDataSink(sink1);

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQueryWithoutStart(qep1);
  ptr->deployQueryWithoutStart(qep2);
  ptr->start();
  source1->start();

  sleep(1);
  ptr->stop();
  testOutput("qep12.txt", joinedExpectedOutput);
}
TEST_F(EngineTest, parallel_same_source_and_sink_test) {
  CompiledTestQueryExecutionPlanPtr qep1(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source1 = createTestSourceWithoutSchema();
  Schema sch1 = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink1 = createBinaryFileSinkWithSchema(sch1, "qep3.txt");
  qep1->setDataSource(source1);
  qep1->setDataSink(sink1);

  CompiledTestQueryExecutionPlanPtr qep2(new CompiledTestQueryExecutionPlan());
  qep2->setDataSource(source1);
  qep2->setDataSink(sink1);

  NodeEngine *ptr = new NodeEngine();
  ptr->deployQueryWithoutStart(qep1);
  ptr->deployQueryWithoutStart(qep2);
  ptr->start();
  source1->start();

  sleep(1);
  ptr->stop();

  testOutput("qep3.txt", joinedExpectedOutput);
}
#endif
}
