#include <atomic>
#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include <Runtime/DataSink.hpp>
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
    static void SetUpTestCase()
    {
        setupLogging();
        IOTDB_INFO("Setup YahooStreamingBenchmarkTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp()
    {
        IOTDB_INFO("Setup YahooStreamingBenchmarkTest test case.");

        Dispatcher::instance();
        BufferManager::instance().setBufferSize(32 * 1024); // 32 kb / 78 bytes = 420 tuples per buffer

        ThreadPool::instance().setNumberOfThreads(8); // 420 tuples per buffer * 8 threads
        ThreadPool::instance().start(8);              // 3360 tuples in parallel

        ysb_source = createYSBSource(1024, 10, false); // 1024 buffers * 32 kb = 32 MB
        ysb_window = createTestWindow(2, 10);          // windows of 2 seconds, 10 campaigns
        ysb_sink = createPrintSink(Schema::create().addField("campaign_id", UINT64).addField("campaign_count", UINT64));
    }

    /* Will be called after a test is executed. */
    void TearDown()
    {
        IOTDB_INFO("Tear down YahooStreamingBenchmarkTest test case.");
        ThreadPool::instance().stop();
        Dispatcher::instance().resetDispatcher();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { IOTDB_INFO("Tear down YahooStreamingBenchmarkTest test class."); }

    DataSourcePtr ysb_source;
    DataSinkPtr ysb_sink;
    WindowPtr ysb_window;

    struct __attribute__((packed)) ysbRecord {

        uint8_t user_id[16];
        uint8_t page_id[16];
        uint8_t campaign_id[16];
        char event_type[9];
        char ad_type[9];
        uint64_t current_ms;
        uint32_t ip;

        ysbRecord()
        {
            event_type[0] = '-'; // invalid record
            current_ms = 0;
            ip = 0;
        }

        ysbRecord(const ysbRecord& rhs)
        {
            memcpy(&user_id, &rhs.user_id, 16);
            memcpy(&page_id, &rhs.page_id, 16);
            memcpy(&campaign_id, &rhs.campaign_id, 16);
            memcpy(&event_type, &rhs.event_type, 9);
            memcpy(&ad_type, &rhs.ad_type, 9);
            current_ms = rhs.current_ms;
            ip = rhs.current_ms;
        }

    }; // size 78 bytes

    struct __attribute__((packed)) ysbRecordResult {
        size_t campaign_id;
        size_t campaign_count;
        size_t last_timestamp;
    }; // 24 bytes

    union tempHash {
        uint64_t value;
        char buffer[8];
    };

  protected:
    static void setupLogging()
    {
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

    class SourceToOutExecutionPlan : public HandCodedQueryExecutionPlan {
      public:
        SourceToOutExecutionPlan(size_t* check_number_windows, size_t* check_number_tuples,
                                 size_t* check_first_timestamp, size_t* check_last_timestamp,
                                 size_t* check_processed_buffers)
            : HandCodedQueryExecutionPlan(), check_number_windows(check_number_windows),
              check_number_tuples(check_number_tuples), check_first_timestamp(check_first_timestamp),
              check_last_timestamp(check_last_timestamp), check_processed_buffers(check_processed_buffers)
        {
        }

        // variables to access checksums
        size_t* check_number_windows;
        size_t* check_number_tuples;
        size_t* check_first_timestamp;
        size_t* check_last_timestamp;
        size_t* check_processed_buffers;

        bool firstPipelineStage(const TupleBuffer&) { return false; }

        bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
        {

            ysbRecord* tuples = (ysbRecord*)buf->buffer;

            // windowing vars
            YSBWindow* window = (YSBWindow*)this->getWindows()[0].get();
            size_t window_size_sec = window->getWindowSizeSec();
            size_t campaign_count = window->getCampaignCount();
            size_t current_window = window->getCurrentWindow();
            size_t last_timestamp = window->getLastTimestamp();
            std::atomic<size_t>** hashTable = window->getHashTable();

            // remember first timestamp for checking
            if (*check_first_timestamp == 0) {
                *check_first_timestamp = time(NULL);
            }

            // other vars
            char key[] = "view";

            // iterate over tuples
            for (size_t i = 0; i < buf->num_tuples; i++) {

                // slow down a bit
                std::this_thread::sleep_for(std::chrono::microseconds(100));

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
                    }
                    else {
                        current_window = 0;
                    }

                    // only do clean up work once -> if not done from other thred before
                    // point of synchronization is a defined field in the hash table
                    if (hashTable[current_window][campaign_count] != timestamp) {
                        atomic_store(&hashTable[current_window][campaign_count], timestamp);

                        // update window values
                        window->setCurrentWindow(current_window);
                        window->setLastTimestamp(timestamp);

                        // print
                        window->print();

                        // sum up checksum
                        size_t checksum = 0;
                        for (size_t bucket = 0; bucket < campaign_count; ++bucket) {
                            checksum += hashTable[old_window][bucket];
                        }

                        // reset hash table for next window
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
                hash_value.value = *(((uint64_t*)tuples[i].campaign_id) + 1);
                uint64_t bucketPos = (hash_value.value * 789 + 321) % campaign_count;
                atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
            }

            // save timestamp of last window change
            (*check_last_timestamp) = last_timestamp;
            (*check_processed_buffers)++;
            return true;
        }
    };
    typedef std::shared_ptr<SourceToOutExecutionPlan> SourceToOutExecutionPlanPtr;

    class SourceToSinkExecutionPlan : public HandCodedQueryExecutionPlan {
      public:
        SourceToSinkExecutionPlan() : HandCodedQueryExecutionPlan() {}

        bool firstPipelineStage(const TupleBuffer&) { return false; }

        bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
        {

            // Input and output buffer
            ysbRecord* inputBufferPtr = (ysbRecord*)buf->buffer;
            TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();
            ysbRecordResult* outputBufferPtr = (ysbRecordResult*)outputBuffer->buffer;
            DataSinkPtr sink = this->getSinks()[0];

            // windowing vars
            YSBWindow* window = (YSBWindow*)this->getWindows()[0].get();
            size_t window_size_sec = window->getWindowSizeSec();
            size_t campaign_count = window->getCampaignCount();
            size_t current_window = window->getCurrentWindow();
            size_t last_timestamp = window->getLastTimestamp();
            std::atomic<size_t>** hashTable = window->getHashTable();

            // other vars
            char key[] = "view";

            // iterate over tuples
            for (size_t i = 0; i < buf->num_tuples; i++) {

                // slow down a bit
                std::this_thread::sleep_for(std::chrono::microseconds(100));

                // filter
                if (strcmp(key, inputBufferPtr[i].event_type) != 0) {
                    continue;
                }

                // windowing
                size_t timestamp = time(NULL);
                if (last_timestamp != timestamp && timestamp % window_size_sec == 0) {

                    // change to new window
                    size_t old_window = current_window;
                    if (current_window == 0) {
                        current_window = 1;
                    }
                    else {
                        current_window = 0;
                    }

                    // only do clean up work once -> if not done from other thred before
                    // point of synchronization is a defined field in the hash table
                    if (hashTable[current_window][campaign_count] != timestamp) {
                        atomic_store(&hashTable[current_window][campaign_count], timestamp);

                        // update window values
                        window->setCurrentWindow(current_window);
                        window->setLastTimestamp(timestamp);

                        // write hash table to output buffer.
                        for (uint32_t campaign = 0; campaign < campaign_count; campaign++) {
                            outputBufferPtr[campaign].campaign_id = campaign;
                            outputBufferPtr[campaign].campaign_count = hashTable[old_window][campaign];
                            outputBufferPtr[campaign].last_timestamp = timestamp;
                        }
                        outputBuffer->num_tuples += campaign_count;
                        outputBufferPtr += campaign_count;

                        // reset hash table for next window
                        std::fill(hashTable[old_window], hashTable[old_window] + campaign_count, 0);
                    }

                    // remember timestammp
                    last_timestamp = timestamp;
                }

                // aggregation
                tempHash hash_value;
                hash_value.value = *(((uint64_t*)inputBufferPtr[i].campaign_id) + 1);
                uint64_t bucketPos = (hash_value.value * 789 + 321) % campaign_count;
                atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
            }
            outputBuffer->tuple_size_bytes = sizeof(ysbRecordResult);
            sink->writeData(outputBuffer);

            return true;
        }
    };
    typedef std::shared_ptr<SourceToSinkExecutionPlan> SourceToSinkExecutionPlanPtr;
};

/* - Source to Out --------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, source_to_out)
{
    IOTDB_INFO("Start source-to-out test.");

    // vars for checks
    size_t check_number_windows = 0;
    size_t check_number_tuples = 0;
    size_t check_first_timestamp = 0;
    size_t check_last_timestamp = 0;
    size_t check_processed_buffers = 0;

    // Run query
    SourceToOutExecutionPlanPtr qep(new SourceToOutExecutionPlan(&check_number_windows, &check_number_tuples,
                                                                 &check_first_timestamp, &check_last_timestamp,
                                                                 &check_processed_buffers));
    qep->addDataSource(ysb_source);
    qep->addWindow(ysb_window);
    Dispatcher::instance().registerQuery(qep);

    while (ysb_source->isRunning() || ysb_source->getNumberOfGeneratedBuffers() != check_processed_buffers) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // is the output correct?
    size_t tmp_tuples = ysb_source->getNumberOfGeneratedTuples() / 3;
    EXPECT_TRUE(tmp_tuples - (tmp_tuples * 0.1) < check_number_tuples ||
                check_number_tuples < tmp_tuples + (tmp_tuples * 0.1));

    // does the windowing work correctly?
    size_t tmp_windows = (check_last_timestamp - check_first_timestamp) / 2;
    EXPECT_TRUE(check_number_windows == tmp_windows || check_number_windows == tmp_windows + 1);
}

/* - Source to Node -------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, source_to_sink)
{
    IOTDB_INFO("Start source-to-sink test.");

    // Run query
    SourceToSinkExecutionPlanPtr qep(new SourceToSinkExecutionPlan());
    qep->addWindow(ysb_window);
    qep->addDataSource(ysb_source);
    qep->addDataSink(ysb_sink);

    Dispatcher::instance().registerQuery(qep);

    while (ysb_source->isRunning() ||
           ysb_source->getNumberOfGeneratedBuffers() != ysb_sink->getNumberOfProcessedBuffers()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

/* - Node to Node ---------------------------------------------------------- */
TEST_F(YahooStreamingBenchmarkTest, node_to_node)
{
    IOTDB_INFO("Start node-to-node test.");

    // TODO: is the output correct?

    // TODO: does the windowing work correctly?

    // TODO: does the approach scale for more buffers?
}
