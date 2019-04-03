#include "gtest/gtest.h"
#include <cassert>
#include <chrono>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <Runtime/YSBWindow.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/YSBGeneratorSource.hpp>
#include <Util/Logger.hpp>
#include <boost/program_options.hpp>
#include <cstring>
#include <memory>
#include <signal.h>
#include <stdio.h>

namespace iotdb {
size_t getTimestamp() { return std::chrono::duration_cast<NanoSeconds>(Clock::now().time_since_epoch()).count(); }

struct __attribute__((packed)) ysbRecord {
    uint8_t user_id[16];
    uint8_t page_id[16];
    uint8_t campaign_id[16];
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

class YSB_SingleNode_PerformanceTest : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;

    YSB_SingleNode_PerformanceTest() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    union tempHash {
        uint64_t value;
        char buffer[8];
    };

    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
    {
        ysbRecord* tuples = (ysbRecord*)buf->buffer;
        size_t lastTimeStamp = time(NULL);
        size_t current_window = 0;
        char key[] = "view";
        size_t windowSizeInSec = 1;
        size_t campaingCnt = 10;
        YSBWindow* window = (YSBWindow*)this->getWindows()[0].get();
        std::atomic<size_t>** hashTable = window->getHashTable();
        for (size_t i = 0; i < buf->num_tuples; i++) {
            if (strcmp(key, tuples[i].event_type) != 0) {
                continue;
            }
            size_t timeStamp = time(NULL);

            if (lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0) {
                // increment to new window
                if (current_window == 0)
                    current_window = 1;
                else
                    current_window = 0;

                if (hashTable[current_window][campaingCnt] != timeStamp) {
                    atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
                    //					window->print();
                    std::fill(hashTable[current_window], hashTable[current_window] + campaingCnt, 0);
                    // memset(myarray, 0, N*sizeof(*myarray)); // TODO: is it faster?
                }

                // TODO: add output result
                lastTimeStamp = timeStamp;
            }

            // consume one tuple
            tempHash hashValue;
            hashValue.value = *(((uint64_t*)tuples[i].campaign_id) + 1);
            uint64_t bucketPos = (hashValue.value * 789 + 321) % campaingCnt;
            atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
        }
        IOTDB_DEBUG("task " << this << " finished processing")
        return true;
    }
};
typedef std::shared_ptr<YSB_SingleNode_PerformanceTest> YSB_SingleNode_PerformanceTestPtr;

int test(size_t toProcessedBuffers, size_t threadCnt, size_t campaignCnt, size_t sourceCnt) {
	return 0;
	YSB_SingleNode_PerformanceTestPtr qep(new YSB_SingleNode_PerformanceTest());

	std::vector<DataSourcePtr> sources;
	for(size_t i = 0; i < sourceCnt; i++)
	{
		sources.push_back(createYSBSource(toProcessedBuffers,campaignCnt, /*pregen*/ true));
		qep->addDataSource(sources[i]);
	}

	WindowPtr window = createTestWindow(campaignCnt,1);
	qep->addWindow(window);

	Dispatcher::instance().registerQuery(qep);

	ThreadPool::instance().setNumberOfThreads(threadCnt);

	size_t start = getTimestamp();
	ThreadPool::instance().start(1);

	size_t endedRuns = 0;
	while(sourceCnt != endedRuns){
		endedRuns = 0;
		for(size_t i = 0; i < sourceCnt; i++)
		{
			if(!sources[i]->isRunning())
			{
				endedRuns++;
			}
		}
//		std::cout << "----- processing current res is:-----" << std::endl;
//		std::cout << "Waiting 1 seconds " << std::endl;
//		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
	size_t end = getTimestamp();
	double elapsed_time = double(end - start) / (1024 * 1024 * 1024);

	size_t processCnt = 0;
	for(size_t i = 0; i < sourceCnt; i++)
	{
		processCnt += sources[i]->getNumberOfGeneratedTuples();
	}

	std::cout << "time=" << elapsed_time << " rec/sec=" << processCnt/elapsed_time
			<< " Processed Buffers=" << toProcessedBuffers
			<< " Processed tuples=" << processCnt
			<< " threads=" << threadCnt
			<< " campaigns="<< campaignCnt
			<< " sources=" << sourceCnt
			<< std::endl;

	ThreadPool::instance().stop();

	Dispatcher::instance().printStatistics(qep);
	Dispatcher::instance().deregisterQuery(qep);
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
    //	logger->setLevel(log4cxx::Level::getDebug());
    logger->setLevel(log4cxx::Level::getInfo());
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

    namespace po = boost::program_options;
    po::options_description desc("Options");
    size_t numBuffers = 1;
    size_t numThreads = 1;
    size_t numCampaings = 1;
    size_t numSources = 1;
    size_t bufferSizeInByte = 4096;

    desc.add_options()("help", "Print help messages")(
        "numBuffers", po::value<uint64_t>(&numBuffers)->default_value(numBuffers), "The number of buffers")(
        "numThreads", po::value<uint64_t>(&numThreads)->default_value(numThreads), "The number of threads")(
        "numCampaings", po::value<uint64_t>(&numCampaings)->default_value(numCampaings), "The number of campaings")(
        "numSources", po::value<uint64_t>(&numSources)->default_value(numSources), "The number of sources")(
        "bufferSizeInByte", po::value<uint64_t>(&bufferSizeInByte)->default_value(bufferSizeInByte),
        "Buffersize in Byte");

    po::variables_map vm;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << "Basic Command Line Parameter " << std::endl << desc << std::endl;
        return 0;
    }

    std::cout << "Settings: "
              << "\nnumBuffers: " << numBuffers << "\nnumThreads: " << numThreads << "\nnumCampaings: " << numCampaings
              << "\nnumSources: " << numSources << "\nBufferSizeInBytes: " << bufferSizeInByte
              << "\nnumTuplesPerBuffer: " << bufferSizeInByte / sizeof(iotdb::ysbRecord) << std::endl;

    iotdb::BufferManager::instance().setBufferSize(bufferSizeInByte);

    iotdb::test(numBuffers, numThreads, numCampaings, numSources);

    return 0;
}
