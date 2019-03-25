#ifndef INCLUDE_UTIL_LOGGER_HPP_
#define INCLUDE_UTIL_LOGGER_HPP_
// TRACE < DEBUG < INFO < WARN < ERROR < FATAL
#include <log4cxx/consoleappender.h>
#include <log4cxx/fileappender.h>
#include <log4cxx/logger.h>
#include <log4cxx/patternlayout.h>

using namespace log4cxx;
using namespace log4cxx::helpers;

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("IOTDB"));

// LoggerPtr logger(Logger::getLogger("IOTDB"));
#define IOTDB_DEBUG(TEXT) LOG4CXX_DEBUG(logger, TEXT)
#define IOTDB_INFO(TEXT) LOG4CXX_INFO(logger, TEXT)
#define IOTDB_ERROR(TEXT) LOG4CXX_ERROR(logger, TEXT)

#endif /* INCLUDE_UTIL_LOGGER_HPP_ */
