#include "gtest/gtest.h"
#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSink.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <Runtime/ZmqSink.hpp>
#include <Runtime/ZmqSource.hpp>

#include <Runtime/PrintSink.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Util/Logger.hpp>
#include <cstring>
#include <memory>
#include <signal.h>
#include <stdio.h>

namespace iotdb {
sig_atomic_t user_wants_to_quit = 0;

void signal_handler(int) { user_wants_to_quit = 1; }

struct __attribute__((packed)) ysbRecord {
    char user_id[16];
    char page_id[16];
    char campaign_id[16];
    char event_type[9];
    char ad_type[9];
    int64_t current_ms;
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

class CompiledYSBZMQOutputTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;

    CompiledYSBZMQOutputTestQueryExecutionPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    union tempHash {
        uint64_t value;
        char buffer[8];
    };

    struct __attribute__((packed)) ysbRecordOut {
        char campaign_id[16];
        char event_type[9];
        int64_t current_ms;
        uint32_t id;
    };

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
    {
        TupleBufferPtr workingBuffer = Dispatcher::instance().getBuffer();
        ysbRecordOut* recBuffer = (ysbRecordOut*)workingBuffer->buffer;
        ysbRecord* tuples = (ysbRecord*)buf->buffer;
        size_t lastTimeStamp = time(NULL);
        size_t current_window = 0;
        char key[] = "view";
        ZmqSink* sink = (ZmqSink*)this->getSinks()[0].get();
        // add code to get corresponding
        size_t qualCnt = 0;
        for (size_t i = 0; i < buf->num_tuples; i++) {
            if (strcmp(key, tuples[i].event_type) != 0) {
                continue;
            }
            memcpy(recBuffer[qualCnt].campaign_id, tuples[i].campaign_id, 16);
            recBuffer[qualCnt].current_ms = tuples[i].current_ms;
            recBuffer[qualCnt].id = tuples[i].ip;
            memcpy(recBuffer[qualCnt].event_type, tuples[i].event_type, 9);
            // write to sink
            qualCnt++;
        }
        workingBuffer->num_tuples = qualCnt;
        workingBuffer->tuple_size_bytes = sizeof(ysbRecordOut);
        sink->writeData(workingBuffer);
        IOTDB_DEBUG("task " << this << " finished processing")
    }
};
typedef std::shared_ptr<CompiledYSBZMQOutputTestQueryExecutionPlan> CompiledYSBZMQOutputTestQueryExecutionPlanPtr;

class CompiledYSBZMQInputTestQueryExecutionPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;

    CompiledYSBZMQInputTestQueryExecutionPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    union tempHash {
        uint64_t value;
        char buffer[8];
    };

    struct __attribute__((packed)) ysbRecordOut {
        char campaign_id[16];
        char event_type[9];
        int64_t current_ms;
        uint32_t id;
    };

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
    {
        TupleBufferPtr workingBuffer = Dispatcher::instance().getBuffer();
        ysbRecordOut* recBuffer = (ysbRecordOut*)workingBuffer->buffer;
        ysbRecordOut* tuples = (ysbRecordOut*)buf->buffer;
        size_t lastTimeStamp = time(NULL);
        size_t current_window = 0;
        char key[] = "view";
        size_t windowSizeInSec = 1;
        size_t campaingCnt = 10;
        YSBPrintSink* sink = (YSBPrintSink*)this->getSinks()[0].get();
        size_t qualCnt = 0;
        for (size_t i = 0; i < buf->num_tuples; i++) {
            if (strcmp(key, tuples[i].event_type) != 0) {
                continue;
            }
            memcpy(recBuffer[qualCnt].campaign_id, tuples[i].campaign_id, 16);
            recBuffer[qualCnt].current_ms = tuples[i].current_ms;
            recBuffer[qualCnt].id = tuples[i].id;
            memcpy(recBuffer[qualCnt].event_type, tuples[i].event_type, 9);
            // write to sink
            qualCnt++;
        }
        workingBuffer->num_tuples = qualCnt;
        workingBuffer->tuple_size_bytes = sizeof(ysbRecordOut);
    }
};
typedef std::shared_ptr<CompiledYSBZMQInputTestQueryExecutionPlan> CompiledYSBZMQInputTestQueryExecutionPlanPtr;

int test()
{
    return true;
    CompiledYSBZMQOutputTestQueryExecutionPlanPtr qep1(new CompiledYSBZMQOutputTestQueryExecutionPlan());
    DataSourcePtr source1 = createYSBSource(1);
    DataSinkPtr sink1 = createZmqSink(source1->getSchema(), "127.0.0.1", 55555, "test");
    DataSourcePtr source2 = createZmqSource(source1->getSchema(), "127.0.0.1", 55555, "test");

    qep1->addDataSource(source1);
    qep1->addDataSource(source2);

    qep1->addDataSink(sink1);
    Dispatcher::instance().registerQuery(qep1);

    //	CompiledYSBZMQInputTestQueryExecutionPlanPtr qep2(new CompiledYSBZMQInputTestQueryExecutionPlan());
    //	DataSourcePtr source2 = createZmqSource(source1->getSchema(), "localhost", 55555, "test");
    //	DataSinkPtr sink2 = createYSBPrintSink(source1->getSchema());
    //	qep2->addDataSource(source2);
    //	qep2->addDataSink(sink2);
    //	Dispatcher::instance().registerQuery(qep2);

    ThreadPool thread_pool;

    thread_pool.start();

    while (source1->isRunning()) {
        std::cout << "----- processing current res is:-----" << std::endl;
        std::cout << "Waiting 1 seconds " << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    //	YSBPrintSink* ySink = (YSBPrintSink*)sink2.get();
    //	std::cout << "printed tuples=" << ySink->getNumberOfPrintedTuples() << std::endl;
    //	if(ySink->getNumberOfPrintedTuples()!= 36)
    //	{
    //		std::cout << "wrong result" << std::endl;
    //		assert(0);
    //	}
    Dispatcher::instance().deregisterQuery(qep1);
    //	Dispatcher::instance().deregisterQuery(qep2);

    thread_pool.stop();
}
} // namespace iotdb

void setupLogging()
{
    // create PatternLayout
    log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

    // create FileAppender
    LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
    log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

    // create ConsoleAppender
    log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

    // set log level
    // logger->setLevel(log4cxx::Level::getTrace());
    logger->setLevel(log4cxx::Level::getDebug());
    //	logger->setLevel(log4cxx::Level::getInfo());
    //	logger->setLevel(log4cxx::Level::getWarn());
    // logger->setLevel(log4cxx::Level::getError());
    //	logger->setLevel(log4cxx::Level::getFatal());

    // add appenders and other will inherit the settings
    logger->addAppender(file);
    logger->addAppender(console);
}

int main(int argc, const char* argv[])
{

    setupLogging();
    iotdb::Dispatcher::instance();

    iotdb::test();

    return 0;
}
