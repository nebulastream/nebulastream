

#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <stdio.h>
#include <signal.h>
#include <Util/Logger.hpp>
#include <memory>
#include <cstring>

namespace iotdb {
sig_atomic_t user_wants_to_quit = 0;

void signal_handler(int) {
user_wants_to_quit = 1;
}

struct __attribute__((packed)) ysbRecord {
	  uint8_t user_id[16];
	  uint8_t page_id[16];
	  uint8_t campaign_id[16];
	  char event_type[9];
	  char ad_type[9];
	  int64_t current_ms;
	  uint32_t ip;

	  ysbRecord(){
		event_type[0] = '-';//invalid record
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

	};//size 78 bytes

class CompiledYSBTestQueryExecutionPlan : public HandCodedQueryExecutionPlan{
public:
    uint64_t count;
    uint64_t sum;
    std::atomic<size_t>** hashTable;
    size_t windowSizeInSec;
    size_t campaingCnt;

    CompiledYSBTestQueryExecutionPlan()
        : HandCodedQueryExecutionPlan(), count(0), sum(0), campaingCnt(10), windowSizeInSec(1){

    }

    void setDataSource(DataSourcePtr source)
	{
	   sources.push_back(source);
	}

    void setWindow(WindowPtr window)
	{
	   windows.push_back(window);
	}


    bool firstPipelineStage(const TupleBuffer&){
        return false;
    }

    union tempHash
    {
    	uint64_t value;
    	char buffer[8];
    };

    void printResult(size_t currenWindow)
    {

    	IOTDB_INFO("Hash Table Content with window " << currenWindow)
    	for(size_t i = 0; i < campaingCnt; i++)
    	{

    		IOTDB_INFO("id=" << i << " cnt=" << hashTable[currenWindow][i])
    	}
    }
    bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf)
    {
        ysbRecord* tuples = (ysbRecord*) buf->buffer;

        size_t lastTimeStamp = time(NULL);
        size_t current_window = 0;
        char key[] = "view";

		for(size_t i = 0; i < buf->num_tuples; i++)
		{
			if(strcmp(key,tuples[i].event_type) != 0)
			{
				continue;
			}
			size_t timeStamp = time(NULL);

			if(lastTimeStamp != timeStamp && timeStamp % windowSizeInSec == 0)
			{
				//increment to new window
				if(current_window == 0)
					current_window = 1;
				else
					current_window = 0;
				if(hashTable[current_window][campaingCnt] != timeStamp)
				{
					IOTDB_INFO("rest st at tup=" << i)
					atomic_store(&hashTable[current_window][campaingCnt], timeStamp);
					printResult(current_window);
					std::fill(hashTable[current_window], hashTable[current_window]+campaingCnt, 0);
					//memset(myarray, 0, N*sizeof(*myarray)); // TODO: is it faster?
				}

				//TODO: add output result
				lastTimeStamp = timeStamp;
			}

		//consume one tuple
			tempHash hashValue;
			hashValue.value = *(((uint64_t*) tuples[i].campaign_id) + 1);
			uint64_t bucketPos = (hashValue.value * 789 + 321)% campaingCnt;
			atomic_fetch_add(&hashTable[current_window][bucketPos], size_t(1));
		}
		IOTDB_DEBUG("task " << this << " finished processing")
    }
};
typedef std::shared_ptr<CompiledYSBTestQueryExecutionPlan> CompiledYSBTestQueryExecutionPlanPtr;


int test() {
	CompiledYSBTestQueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());
	DataSourcePtr source = createYSBSource();
	WindowPtr window = createTestWindow();
	qep->setDataSource(source);
	qep->setWindow(window);

	Dispatcher::instance().registerQuery(qep);

	ThreadPool thread_pool;

	thread_pool.start();

//	std::cout << "Waiting 2 seconds " << std::endl;
//	std::this_thread::sleep_for(std::chrono::seconds(3));

	while(!user_wants_to_quit)
	{
	  std::this_thread::sleep_for(std::chrono::seconds(2));
	  std::cout << "waiting to finish" << std::endl;
	}

	Dispatcher::instance().deregisterQuery(qep);

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
	//logger->setLevel(log4cxx::Level::getTrace());
	logger->setLevel(log4cxx::Level::getDebug());
	//logger->setLevel(log4cxx::Level::getIngfo());
//	logger->setLevel(log4cxx::Level::getWarn());
	//logger->setLevel(log4cxx::Level::getError());
//	logger->setLevel(log4cxx::Level::getFatal());

	// add appenders and other will inherit the settings
	logger->addAppender(file);
	logger->addAppender(console);
}

int main(int argc, const char *argv[]) {

	setupLogging();
  iotdb::Dispatcher::instance();

  iotdb::test();


  return 0;
}
