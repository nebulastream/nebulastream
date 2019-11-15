#include <map>
#include <vector>

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <log4cxx/appender.h>
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <QueryLib/WindowManagerLib.hpp>
#include <API/Window/WindowDefinition.hpp>
#include <API/Window/WindowAggregation.hpp>
#include <random>
#include <NodeEngine/BufferManager.hpp>

#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>

namespace iotdb {
class WindowManagerTest : public testing::Test {
 public:
  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup WindowMangerTest test class.");
  }

  static void TearDownTestCase() { std::cout << "Tear down WindowManager test class." << std::endl; }

  const size_t buffers_managed = 10;
  const size_t buffer_size = 4 * 1024;
 protected:
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(
        new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "WindowManager.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    // logger->setLevel(log4cxx::Level::getDebug());
    logger->setLevel(log4cxx::Level::getInfo());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }

};

class TestAggregation : public WindowAggregation {
 public:
  TestAggregation() : WindowAggregation() {};
  void compileLiftCombine(CompoundStatementPtr currentCode,
                          BinaryOperatorStatement partialRef,
                          StructDeclaration inputStruct,
                          BinaryOperatorStatement inputRef){};
};

TEST_F(WindowManagerTest, sum_aggregation_test) {
  auto field =createField("test",4);
  const WindowAggregationPtr aggregation = Sum::on(Field(field));
  if(Sum* store = dynamic_cast<Sum*>(aggregation.get())) {
    auto partial = store->lift<int64_t,int64_t>(1L);
    auto partial2 = store->lift<int64_t,int64_t>(2L);
    auto combined = store->combine<int64_t>(partial, partial2);
    auto final = store->lower<int64_t, int64_t>(combined);
    ASSERT_EQ(final, 3);
  }


}


TEST_F(WindowManagerTest, check_slice) {
  auto store = new WindowSliceStore<int64_t>(0L);
  auto aggregation = std::make_shared<TestAggregation>(TestAggregation());

  auto windowDef = std::make_shared<WindowDefinition>(WindowDefinition(aggregation, TumblingWindow::of(Seconds(60))));

  auto windowManager = new WindowManager(windowDef);
  uint64_t ts = 10;

  windowManager->sliceStream(ts, store);
  auto sliceIndex = store->getSliceIndexByTs(ts);
  auto &aggregates = store->getPartialAggregates();
  aggregates[sliceIndex]++;

  sliceIndex = store->getSliceIndexByTs(ts);
  aggregates = store->getPartialAggregates();
  aggregates[sliceIndex]++;
 // std::cout << aggregates[sliceIndex] << std::endl;
  //ASSERT_EQ(buffers_count, buffers_managed);
  ASSERT_EQ(aggregates[sliceIndex], 2);
}

}