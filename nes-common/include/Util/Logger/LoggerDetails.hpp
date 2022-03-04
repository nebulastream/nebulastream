#ifndef NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_

#include <Util/Logger/Logger.hpp>
#include <log4cxx/logger.h>
namespace NES {

class LoggerDetails {
  public:
    LoggerDetails();
    void setLogLevel(const LogLevel logLevel);
    void setupLogging(const std::string& logFileName);
    void setThreadName(const std::string threadName);
    log4cxx::LoggerPtr loggerPtr;
};

}// namespace NES

#endif//NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
