#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/Window.hpp>
#include <Runtime/YSBWindow.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>

using namespace iotdb;

class YahooStreamingBenchmarkTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() {
    setupLogging();
    IOTDB_INFO("Setup YahooStreamingBenchmarkTest test class.");
  }

  /* Will be called before a test is executed. */
  void SetUp() {
    IOTDB_INFO("Setup YahooStreamingBenchmarkTest test case.");
    Dispatcher::instance();
    BufferManager::instance().setBufferSize(10 * 1024); // 10 kb / 78 bytes = 131 tuples per buffer
    ysb_source = createYSBSource(256, 10, true);        // 256 buffers * 4 kb = 1 mb
    ThreadPool::instance().setNumberOfThreads(8);       // 131 tuples per buffer * 8 threads = 1048 tuples in parallel
    ThreadPool::instance().start(8);                    // 1048 tuples in parallel
    ysb_window = createTestWindow(2, 10);               // windows of 2 seconds, 10 campaigns
  }

  /* Will be called after a test is executed. */
  void TearDown() {
    IOTDB_INFO("Tear down YahooStreamingBenchmarkTest test case.");
    ThreadPool::instance().stop();
    Dispatcher::instance().resetDispatcher();
  }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { IOTDB_INFO("Tear down YahooStreamingBenchmarkTest test class."); }

  DataSourcePtr ysb_source;
  WindowPtr ysb_window;

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
  static void setupLogging() {
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "YahooStreamingBenchmarkTest.log");
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

/* - Source to Out --------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, source_to_out) {

  class SourceToOutExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    SourceToOutExecutionPlan(size_t *check_number_windows, size_t *check_number_tuples, size_t *check_first_timestamp,
                             size_t *check_last_timestamp)
        : HandCodedQueryExecutionPlan(), check_number_windows(check_number_windows),
          check_number_tuples(check_number_tuples), check_first_timestamp(check_first_timestamp),
          check_last_timestamp(check_last_timestamp) {}

    union tempHash {
      uint64_t value;
      char buffer[8];
    };

    // variables to access checksums
    size_t *check_number_windows;
    size_t *check_number_tuples;
    size_t *check_first_timestamp;
    size_t *check_last_timestamp;

    bool firstPipelineStage(const TupleBuffer &) { return false; }

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) {

      ysbRecord *tuples = (ysbRecord *)buf->buffer;

      // windowing vars
      YSBWindow *window = (YSBWindow *)this->getWindows()[0].get();
      size_t window_size_sec = window->getWindowSizeSec();
      size_t campaign_count = window->getCampaignCount();
      size_t current_window = window->getCurrentWindow();
      size_t last_timestamp = window->getLastTimestamp();
      std::atomic<size_t> **hashTable = window->getHashTable();

      // remember first timestamp for checking
      if (*check_first_timestamp == 0) {
        *check_first_timestamp = time(NULL);
      }

      // other vars
      char key[] = "view";

      // iterate over tuples
      for (size_t i = 0; i < buf->num_tuples; i++) {

        // filter
        if (strcmp(key, tuples[i].event_type) != 0) {
          continue;
        }

        // windowing
        size_t timestamp = time(NULL);
        if (last_timestamp != timestamp && timestamp % window_size_sec == 0) {

          // change to new window
          size_t old_window = current_window;
          if (current_window == 0) {
            current_window = 1;
          } else {
            current_window = 0;
          }

          // only do clean up work once -> if not done from other thred before
          // point of synchronization is a defined field in the hash table
          if (hashTable[current_window][campaign_count] != timestamp) {
            atomic_store(&hashTable[current_window][campaign_count], timestamp);

            // update window values
            window->setCurrentWindow(current_window);
            window->setLastTimestamp(timestamp);

            // reset hash table for next window
            size_t checksum = 0;
            for (size_t bucket = 0; bucket < campaign_count; ++bucket) {
              checksum += hashTable[old_window][bucket];
            }
            std::fill(hashTable[old_window], hashTable[old_window] + campaign_count, 0);

            // increment number of windows checksum
            (*check_number_windows) += 1;
            (*check_number_tuples) += checksum;
          }

          // remember timestammp
          last_timestamp = timestamp;
        }

        // aggregation
        tempHash hash_value;
        hash_value.value = *(((uint64_t *)tuples[i].campaign_id) + 1);
        uint64_t bucketPos = (hash_value.value * 789 + 321) % campaign_count;
        atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }

      // save timestamp of last window change
      (*check_last_timestamp) = last_timestamp;

      return true;
    }
  };
  typedef std::shared_ptr<SourceToOutExecutionPlan> SourceToOutExecutionPlanPtr;

  IOTDB_INFO("Start source-to-out test.");

  // vars for checks
  size_t check_number_windows = 0;
  size_t check_number_tuples = 0;
  size_t check_first_timestamp = 0;
  size_t check_last_timestamp = 0;

  // Run query
  SourceToOutExecutionPlanPtr qep(new SourceToOutExecutionPlan(&check_number_windows, &check_number_tuples,
                                                               &check_first_timestamp, &check_last_timestamp));
  qep->addDataSource(ysb_source);
  qep->addWindow(ysb_window);
  Dispatcher::instance().registerQuery(qep);

  while (ysb_source->isRunning()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  std::cout << "Number of Windows processed: " << check_number_windows << std::endl;
  std::cout << "Number of Tuples processed: " << check_number_tuples << std::endl;
  std::cout << "First timestamp: " << check_first_timestamp << std::endl;
  std::cout << "Last timestamp: " << check_last_timestamp << std::endl;

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
