#ifndef INCLUDE_UTIL_LOGGER_HPP_
#define INCLUDE_UTIL_LOGGER_HPP_
// TRACE < DEBUG < INFO < WARN < ERROR < FATAL

#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <iostream>

using namespace log4cxx;
using namespace log4cxx::helpers;

namespace NES {

enum DebugLevel {
  LOG_NONE,
  LOG_WARNING,
  LOG_DEBUG,
  LOG_INFO,
  LOG_TRACE
};

static std::string getDebugLevelAsString(DebugLevel level) {
  if (level == LOG_NONE) {
    return "LOG_NONE";
  } else if (level == LOG_WARNING) {
    return "LOG_WARNING";
  } else if (level == LOG_DEBUG) {
    return "LOG_DEBUG";
  } else if (level == LOG_INFO) {
    return "LOG_INFO";
  } else if (level == LOG_TRACE) {
      return "LOG_TRACE";
  } else {
    return "UNKNOWN";
  }
}

static log4cxx::LoggerPtr NESLogger(log4cxx::Logger::getLogger("NES"));

// LoggerPtr logger(Logger::getLogger("NES"));

#define NES_TRACE(TEXT) LOG4CXX_TRACE(NESLogger, TEXT)
#define NES_DEBUG(TEXT) LOG4CXX_DEBUG(NESLogger, TEXT)
#define NES_INFO(TEXT) LOG4CXX_INFO(NESLogger, TEXT)
#define NES_WARNING(TEXT) LOG4CXX_WARN(NESLogger, TEXT)
#define NES_ERROR(TEXT) LOG4CXX_ERROR(NESLogger, TEXT)
#define NES_FATAL_ERROR(TEXT) LOG4CXX_ERROR(NESLogger, TEXT)

static void setupLogging(std::string logFileName, DebugLevel level) {
  std::cout << "SETUP_LOGGING" << std::endl;
  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, logFileName);
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  // set log level
  if (level == LOG_NONE) {
    NESLogger->setLevel(log4cxx::Level::getOff());
  } else if (level == LOG_WARNING) {
    NESLogger->setLevel(log4cxx::Level::getWarn());
  } else if (level == LOG_DEBUG) {
    NESLogger->setLevel(log4cxx::Level::getDebug());
  } else if (level == LOG_INFO) {
    NESLogger->setLevel(log4cxx::Level::getInfo());
  } else if (level == LOG_TRACE) {
      NESLogger->setLevel(log4cxx::Level::getTrace());
  } else {
    NES_ERROR(
        "setupLogging: log level not supported " << getDebugLevelAsString(level))
    throw Exception("Error while setup logging");
  }

  NESLogger->addAppender(file);
  NESLogger->addAppender(console);
}

#define NES_NOT_IMPLEMENTED NES_ERROR("Function Not Implemented!") throw Exception("not implemented");

}

#endif /* INCLUDE_UTIL_LOGGER_HPP_ */
