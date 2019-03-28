#include "gtest/gtest.h"
#include <cassert>
#include <iostream>

#include <Core/DataTypes.hpp>

#include <CodeGen/HandCodedQueryExecutionPlan.hpp>
#include <Core/TupleBuffer.hpp>

#include <Runtime/DataSource.hpp>
#include <Runtime/Window.hpp>
#include <Runtime/YSBWindow.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Util/Logger.hpp>
#include <cstring>
#include <QEPs/CompiledYSBTestQueryExecutionPlan.hpp>

namespace iotdb {
sig_atomic_t user_wants_to_quit = 0;

void signal_handler(int) { user_wants_to_quit = 1; }


int test() {
	CompiledYSBTestQueryExecutionPlanPtr qep(new CompiledYSBTestQueryExecutionPlan());
	DataSourcePtr source = createYSBSource(1000,10, /*pregen*/ false);
	WindowPtr window = createTestWindow(10,2);
	qep->addDataSource(source);
	qep->addWindow(window);
    YSBWindow* res_window = (YSBWindow*)qep->getWindows()[0].get();

    Dispatcher::instance().registerQuery(qep);

	ThreadPool::instance().start(1);
	while(source->isRunning()){
//		std::cout << "----- processing current res is:-----" << std::endl;
//		res_window->print();
		std::cout << "Waiting 1 seconds " << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(1));

	}

    size_t sum = 0;
    for (size_t i = 0; i < 10; i++) {
        sum += res_window->getHashTable()[0][i];
        sum += res_window->getHashTable()[1][i];
    }
    std::cout << " ========== FINAL query result  ========== " << sum << std::endl;
    if (sum != 18000) {
        std::cout << "wrong result" << std::endl;
        //    	assert(0);
    }
    else
    {
        std::cout << "right result" << std::endl;
    }
	EXPECT_EQ(sum, 18000);

	Dispatcher::instance().deregisterQuery(qep);

//	while(!user_wants_to_quit)
//	{
//	  std::this_thread::sleep_for(std::chrono::seconds(2));
//	  std::cout << "waiting to finish" << std::endl;
//	}

    Dispatcher::instance().deregisterQuery(qep);

    //	while(!user_wants_to_quit)
    //	{
    //	  std::this_thread::sleep_for(std::chrono::seconds(2));
    //	  std::cout << "waiting to finish" << std::endl;
    //	}

    ThreadPool::instance().stop();
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

    iotdb::test();

    return 0;
}
