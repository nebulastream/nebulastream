

#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/compiledYSBPlan.hpp>
#include <stdio.h>
#include <signal.h>
#include <Util/Logger.hpp>

namespace iotdb {
sig_atomic_t user_wants_to_quit = 0;

void signal_handler(int) {
user_wants_to_quit = 1;
}
int test() {
	CompiledYSBTestQueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());
	DataSourcePtr source = createYSBSource();
	qep->setDataSource(source);

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
//	logger->setLevel(log4cxx::Level::getDebug());
	//logger->setLevel(log4cxx::Level::getIngfo());
//	logger->setLevel(log4cxx::Level::getWarn());
	//logger->setLevel(log4cxx::Level::getError());
	logger->setLevel(log4cxx::Level::getFatal());

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
