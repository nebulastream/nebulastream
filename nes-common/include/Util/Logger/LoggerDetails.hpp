#ifndef NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_

#include <Util/Logger/Logger.hpp>
#include <log4cxx/logger.h>
namespace NES {

/**
 * @brief Logger Details maintains a reference to the log4cxx logger.
 * The main purpose for this class is to hide details about the the log implementation from consumers of the Logger.hpp.
 */
class LoggerDetails {
  public:
    LoggerDetails();
    /**
     * @brief Sets a specific log level to log4cxx.
     * @param logLevel
     */
    void setLogLevel(const LogLevel logLevel);

    /**
     * @brief Setup the basic logger and appends the console and file appender.
     * @param logFileName file name of the log file
     */
    void setupLogging(const std::string& logFileName);

    /**
     * @brief Sets the thread name
     * @param threadName
     */
    void setThreadName(const std::string threadName);
    log4cxx::LoggerPtr loggerPtr;
};

}// namespace NES

#endif//NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
