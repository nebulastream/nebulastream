
#include <cassert>
#include <iostream>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Util/Logger.hpp>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <SourceSink/SinkCreator.hpp>

#define DEBUG_OUTPUT
namespace iotdb {

class CompiledTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
 public:
uint64_t count;
uint64_t sum;
CompiledTestQueryExecutionPlan()
    : HandCodedQueryExecutionPlan(),
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
  uint64_t* tuples = (uint64_t*) buf->getBuffer();

  IOTDB_INFO("Test: Start execution");

  for (size_t i = 0; i < buf->getNumberOfTuples(); ++i) {
    count++;
    sum += tuples[i];
  }

  IOTDB_INFO("Test: query result = Processed Block:" << buf->getNumberOfTuples() << " count: " << count
            << "sum: " << sum )
  assert(sum == 10);

  DataSinkPtr sink = this->getSinks()[0];
//  sink->getSchema().getSchemaSize();
  TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
  u_int32_t* arr = (u_int32_t*) outputBuffer->getBuffer();
  arr[0] = sum;
  outputBuffer->setNumberOfTuples(1);
  outputBuffer->setTupleSizeInBytes(4);
//  ysbRecordResult* outputBufferPtr = (ysbRecordResult*)outputBuffer->buffer;
  sink->writeData(outputBuffer);
  return true;
}
};
typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

/**
 * @brief test for the engine
 * TODO: add more test cases
 */
class EngineTest : public testing::Test {
 public:
  static void SetUpTestCase() {
#ifdef DEBUG_OUTPUT
    setupLogging();
#endif
    IOTDB_INFO("Setup EngineTest test class.");
  }
  static void TearDownTestCase() {
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
     logger->setLevel(log4cxx::Level::getDebug());
//    logger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }

};

TEST_F(EngineTest, start_stop_engine_empty) {
  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->stop();
}

TEST_F(EngineTest, deploy_start_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  Schema sch = Schema::create().addField("sum", 4);
//  Schema& sch = Schema::create().addField("sum", BasicType::UINT32);
  DataSinkPtr sink = createBinaryFileSinkWithSchema(sch, "file.txt");
  qep->setDataSource(source);
  qep->setDataSink(sink);

  NodeEngine* ptr = new NodeEngine();
  ptr->deployQuery(qep);
  ptr->start();
  sleep(100);
//  ptr->stop();
}
#ifdef 0

TEST_F(EngineTest, start_deploy_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
}

TEST_F(EngineTest, start_deploy_undeploy_stop_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->undeployQuery(qep);
  ptr->stop();
}

TEST_F(EngineTest, startWithRedeploy_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
  ptr->startWithRedeploy();
}

TEST_F(EngineTest, stopWithRedeploy_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stopWithUndeploy();
}

TEST_F(EngineTest, resetQEP_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->resetQEPs();
}

TEST_F(EngineTest, change_dop_with_restart_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->setDOPWithRestart(2);
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
}

TEST_F(EngineTest, change_dop_without_restart_test) {
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSourceWithoutSchema();
  qep->setDataSource(source);

  NodeEngine* ptr = new NodeEngine();
  ptr->setDOPWithoutRestart(2);
  ptr->start();
  ptr->deployQuery(qep);
  sleep(1);
  ptr->stop();
  ptr->start();
  EXPECT_EQ(ptr->getDOP(), size_t(2));
}



#endif
}
