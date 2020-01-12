#ifndef INCLUDE_UTIL_LOGGER_HPP_
#define INCLUDE_UTIL_LOGGER_HPP_
// TRACE < DEBUG < INFO < WARN < ERROR < FATAL
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>

#include <iostream>

using namespace log4cxx;
using namespace log4cxx::helpers;

namespace NES {
static log4cxx::LoggerPtr iotdbLogger(log4cxx::Logger::getLogger("IOTDB"));

// LoggerPtr logger(Logger::getLogger("IOTDB"));
#define IOTDB_DEBUG(TEXT) LOG4CXX_DEBUG(iotdbLogger, TEXT)
#define IOTDB_INFO(TEXT) LOG4CXX_INFO(iotdbLogger, TEXT)
#define IOTDB_WARNING(TEXT) LOG4CXX_WARN(iotdbLogger, TEXT)
#define IOTDB_RES(TEXT) LOG4CXX_WARN(iotdbLogger, TEXT)
#define IOTDB_ERROR(TEXT) LOG4CXX_ERROR(iotdbLogger, TEXT)
#define IOTDB_FATAL_ERROR(TEXT) LOG4CXX_ERROR(iotdbLogger, TEXT)

static inline void setupLogger(log4cxx::LayoutPtr layoutPtr = nullptr,
                               log4cxx::AppenderPtr appenderPtr = nullptr,
                               log4cxx::LevelPtr levelPtr = log4cxx::Level::getInfo()) {
  if (layoutPtr == nullptr) {
    std::cout << "INFO: using default Pattern Layout" << std::endl;
    layoutPtr = new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n");
  }
  if (appenderPtr == nullptr) {
    std::cout << "INFO: using console as default appenderPtr" << std::endl;
    appenderPtr = new log4cxx::ConsoleAppender(layoutPtr);
  }

  iotdbLogger->setLevel(levelPtr);
  iotdbLogger->addAppender(appenderPtr);
}
//TODO:add throw exception
#define IOTDB_NOT_IMPLEMENTED IOTDB_ERROR("Function Not Implemented!") throw Exception("not implemented");

}

#endif /* INCLUDE_UTIL_LOGGER_HPP_ */
