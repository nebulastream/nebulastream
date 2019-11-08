
#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <NodeEngine/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Util/Logger.hpp>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>

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
bool firstPipelineStage(const TupleBuffer&) {
  return false;
}

bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) {
  uint64_t* tuples = (uint64_t*) buf->buffer;

  IOTDB_INFO("Test: Start execution");

  for (size_t i = 0; i < buf->num_tuples; ++i) {
    count++;
    sum += tuples[i];
  }

  IOTDB_INFO("Test: query result = Processed Block:" << buf->num_tuples << " count: " << count
            << "sum: " << sum )
  assert(sum == 512);
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

TEST_F(EngineTest, start_stop_engine_one_query) {
  NodeEngine* ptr = new NodeEngine();
  CompiledTestQueryExecutionPlanPtr qep(new CompiledTestQueryExecutionPlan());
  DataSourcePtr source = createTestSource();
  qep->setDataSource(source);
  ptr->deployQuery(qep);
  ptr->start();
  sleep(1);
  ptr->stop();
}

}
