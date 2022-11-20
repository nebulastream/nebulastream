/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
#define NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_

#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <log4cxx/logger.h>

namespace NES::detail {

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

    /**
    * @brief Outputs a log message with a specific log level and location.
    * @param logLevel LogLevel
    * @param message log message
    * @param location source code location
    */
    void log(const LogLevel& logLevel, const std::string& message, const std::source_location location);

  private:
    log4cxx::LoggerPtr loggerPtr;
    std::atomic<bool> isRegistered{false};
};

}// namespace NES::detail

#endif // NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGERDETAILS_HPP_
