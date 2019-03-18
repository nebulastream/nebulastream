#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/Window.hpp>

using namespace iotdb;

class YahooStreamingBenchmarkTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    setupLogging();
    IOTDB_DEBUG("Setup YahooStreamingBenchmarkTest test class.");
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    IOTDB_DEBUG("Setup YahooStreamingBenchmarkTest test case.");
    ysb_source = createYSBSource(1000);
    ysb_window = createTestWindow();
    Dispatcher::instance();
    thread_pool.start();
  }

  /* Will be called after a test is executed. */
  void TearDown() {
    IOTDB_DEBUG("Tear down YahooStreamingBenchmarkTest test case.");
    thread_pool.stop();
    Dispatcher::instance().resetDispatcher();
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { IOTDB_DEBUG("Tear down YahooStreamingBenchmarkTest test class."); }

  struct __attribute__((packed)) ysbRecord {

    uint8_t user_id[16];
    uint8_t page_id[16];
    uint8_t campaign_id[16];
    char event_type[9];
    char ad_type[9];
    uint64_t current_ms;
    uint32_t ip;

    ysbRecord() {
      event_type[0] = '-'; // invalid record
      current_ms = 0;
      ip = 0;
    }

    ysbRecord(const ysbRecord &rhs) {
      memcpy(&user_id, &rhs.user_id, 16);
      memcpy(&page_id, &rhs.page_id, 16);
      memcpy(&campaign_id, &rhs.campaign_id, 16);
      memcpy(&event_type, &rhs.event_type, 9);
      memcpy(&ad_type, &rhs.ad_type, 9);
      current_ms = rhs.current_ms;
      ip = rhs.current_ms;
    }

  }; // size 78 bytes

private:
  ThreadPool thread_pool;
  DataSourcePtr ysb_source;
  WindowPtr ysb_window;

  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "YahooStreamingBenchmarkTest.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    logger->setLevel(log4cxx::Level::getDebug());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
  }
};

/* - Source to Out --------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, source_to_out) {
  IOTDB_INFO("Start source-to-out test.");

  // TODO: is the output correct?

  // TODO: does the windowing work correctly?

  // TODO: does the approach scale for more buffers?
}

/* - Source to Node -------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, source_to_sink) {
  IOTDB_INFO("Start source-to-sink test.");

  // TODO: is the output correct?

  // TODO: does the windowing work correctly?

  // TODO: does the approach scale for more buffers?
}

/* - Node to Node ---------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, node_to_node) {
  IOTDB_INFO("Start node-to-node test.");

  // TODO: is the output correct?

  // TODO: does the windowing work correctly?

  // TODO: does the approach scale for more buffers?
}
