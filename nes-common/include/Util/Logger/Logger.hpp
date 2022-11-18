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

#ifndef NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
#define NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
#include <Exceptions/NotImplementedException.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/StacktraceLoader.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <memory>
#include <sstream>

namespace NES {

/**
 * @brief Indicators for a log level following the priority of log messages.
 * A specific log level contains all log messages of an lower level.
 * For example if LOG_LEVEL is LOG_WARNING, then it also contains LOG_NONE, LOG_FATAL_ERROR, and LOG_ERROR.
 */
enum class LogLevel : uint8_t {
    // Indicates that no information will be logged.
    LOG_NONE = 1,
    // Indicates that only information about fatal errors will be logged.
    LOG_FATAL_ERROR = 2,
    // Indicates that all kinds of error messages will be logged.
    LOG_ERROR = 3,
    // Indicates that all warnings and error messages will be logged.
    LOG_WARNING = 4,
    // Indicates that additional debug messages will be logged.
    LOG_INFO = 5,
    // Indicates that additional information will be logged.
    LOG_DEBUG = 6,
    // Indicates that all available information will be logged (can result in massive output).
    LOG_TRACE = 7
};

// In the following we define the NES_COMPILE_TIME_LOG_LEVEL macro.
// This macro indicates the log level, which was chooses at compilation time and enables the complete
// elimination of log messages.
#if defined(NES_LOGLEVEL_TRACE)
#define NES_COMPILE_TIME_LOG_LEVEL 7
#elif defined(NES_LOGLEVEL_DEBUG)
#define NES_COMPILE_TIME_LOG_LEVEL 6
#elif defined(NES_LOGLEVEL_INFO)
#define NES_COMPILE_TIME_LOG_LEVEL 5
#elif defined(NES_LOGLEVEL_WARN)
#define NES_COMPILE_TIME_LOG_LEVEL 4
#elif defined(NES_LOGLEVEL_ERROR)
#define NES_COMPILE_TIME_LOG_LEVEL 3
#elif defined(NES_LOGLEVEL_FATAL_ERROR)
#define NES_COMPILE_TIME_LOG_LEVEL 2
#elif defined(NES_LOGLEVEL_NONE)
#define NES_COMPILE_TIME_LOG_LEVEL 1
#endif

/**
 * @brief GetLogLevel returns the integer LogLevel value for an specific LogLevel value.
 * @param value LogLevel
 * @return integer between 1 and 7 to identify the log level.
 */
constexpr uint64_t getLogLevel(const LogLevel value) { return magic_enum::enum_integer(value); }

/**
 * @brief getLogName returns the string representation LogLevel value for a specific LogLevel value.
 * @param value LogLevel
 * @return string of value
 */
constexpr auto getLogName(const LogLevel value) { return magic_enum::enum_name(value); }

namespace detail {
class LoggerDetails;
using LoggerDetailsPtr = std::unique_ptr<LoggerDetails>;
}// namespace detail

/**
 * @brief Central logger, which allows to output different log messages to the console and files.
 * This component holds a reference to the central logger via a singleton.
 * @note The actual logging is implemented in LoggerDetails.
 * Logger is just a wrapper to hide the log4cxx headers from the remaining system.
 */
class Logger {
  public:
    /**
     * @brief Returns the central instance to the logger.
     * @note As this is a singleton the returned pointer is assumed to be valid.
     * @return Logger*
     */
    static Logger* getInstance();

    /**
     * @brief Initializes the logger and appends all log messages to a file and the console.
     * @param logFileName the log file.
     * @param level the desired log level.
     */
    static void setupLogging(const std::string& logFileName, LogLevel level);

    /**
     * @brief Outputs a log message with a specific log level and location.
     * @param logLevel LogLevel
     * @param message log message
     * @param location source code location
     */
    void log(const LogLevel& logLevel,
             const std::string& message,
             const std::source_location location = std::source_location::current());

    /**
     * @brief Returns the current runtime log level.
     * @return LogLevel
     */
    LogLevel& getCurrentLogLevel();

    /**
     * @brief Sets a new runtime log level.
     * @note The runtime log level should to be lower then the NES_COMPILE_TIME_LOG_LEVEL.
     * @param logLevel
     */
    void setLogLevel(const LogLevel logLevel);

    /**
     * @brief Theads the current thread name to the logger
     * @param threadName current thread name
     */
    void setThreadName(const std::string threadName);

  private:
    /**
     * @brief Private constructor for the logger. Is only called by the getInstance function.
     */
    Logger();
    LogLevel currentLogLevel;
    detail::LoggerDetailsPtr details;
};

#define NES_LOG(LEVEL, message)                                                                                                  \
    do {                                                                                                                         \
        auto constexpr __level = NES::getLogLevel(LEVEL);                                                                        \
        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= __level) {                                                                   \
            auto __logger = NES::Logger::getInstance();                                                                          \
            if (NES::getLogLevel(__logger->getCurrentLogLevel()) >= __level) {                                                   \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << message;                                                                                                 \
                __logger->log(LEVEL, __buffer.str());                                                                            \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

// Creates a log message with log level trace.
#define NES_TRACE(...) NES_LOG(NES::LogLevel::LOG_TRACE, __VA_ARGS__);
// Creates a log message with log level info.
#define NES_INFO(...) NES_LOG(NES::LogLevel::LOG_INFO, __VA_ARGS__);
// Creates a log message with log level debug.
#define NES_DEBUG(...) NES_LOG(NES::LogLevel::LOG_DEBUG, __VA_ARGS__);
// Creates a log message with log level warning.
#define NES_WARNING(...) NES_LOG(NES::LogLevel::LOG_WARNING, __VA_ARGS__);
// Creates a log message with log level error.
#define NES_ERROR(...) NES_LOG(NES::LogLevel::LOG_ERROR, __VA_ARGS__);
// Creates a log message with log level fatal error.
#define NES_FATAL_ERROR(...) NES_LOG(NES::LogLevel::LOG_FATAL_ERROR, __VA_ARGS__);

/// I am aware that we do not like __ before variable names but here we need them
/// to avoid name collions, e.g., __buffer, __stacktrace
/// that should not be a problem because of the scope, however, better be safe than sorry :P
#ifdef NES_DEBUG_MODE
//Note Verify is only evaluated in Debug but not in Release
#define NES_VERIFY(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            NES_ERROR("NES Fatal Error on " #CONDITION << " message: " << TEXT);                                                 \
            {                                                                                                                    \
                auto __stacktrace = NES::collectAndPrintStacktrace();                                                            \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << TEXT;                                                                              \
                NES::Exceptions::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)
#else
#define NES_VERIFY(CONDITION, TEXT) ((void) 0)
#endif

#define NES_ASSERT(CONDITION, TEXT)                                                                                              \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            NES_ERROR("NES Fatal Error on " #CONDITION << " message: " << TEXT);                                                 \
            {                                                                                                                    \
                auto __stacktrace = NES::collectAndPrintStacktrace();                                                            \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << TEXT;                                                                              \
                NES::Exceptions::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

#define NES_ASSERT2_FMT(CONDITION, ...)                                                                                          \
    do {                                                                                                                         \
        if (!(CONDITION)) {                                                                                                      \
            NES_ERROR("NES Fatal Error on " #CONDITION << " message: " << __VA_ARGS__);                                          \
            {                                                                                                                    \
                auto __stacktrace = NES::collectAndPrintStacktrace();                                                            \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << "Failed assertion on " #CONDITION;                                                                       \
                __os << " error message: " << __VA_ARGS__;                                                                       \
                NES::Exceptions::invokeErrorHandlers(__buffer.str(), std::move(__stacktrace));                                   \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

#define NES_THROW_RUNTIME_ERROR(...)                                                                                             \
    do {                                                                                                                         \
        auto __stacktrace = NES::collectAndPrintStacktrace();                                                                    \
        std::stringbuf __buffer;                                                                                                 \
        std::ostream __os(&__buffer);                                                                                            \
        __os << __VA_ARGS__;                                                                                                     \
        throw Exceptions::RuntimeException(__buffer.str(), std::move(__stacktrace));                                             \
    } while (0)

#define NES_NOT_IMPLEMENTED()                                                                                                    \
    do {                                                                                                                         \
        throw Exceptions::NotImplementedException("not implemented");                                                            \
    } while (0)

}// namespace NES

#endif// NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
