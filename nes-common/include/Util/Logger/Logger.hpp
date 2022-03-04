#ifndef NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
#define NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
#include <Exceptions/NotImplementedException.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/StacktraceLoader.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
namespace NES {

enum class LogLevel : uint8_t {
    LOG_NONE = 1,
    LOG_FATAL_ERROR = 2,
    LOG_ERROR = 3,
    LOG_WARNING = 4,
    LOG_DEBUG = 5,
    LOG_INFO = 6,
    LOG_TRACE = 7
};

// In the following we define the NES_COMPILE_TIME_LOG_LEVEL macro.
// This macro indicates the log level, which was chooses at compilation time and enables the complete
// elimination of log messages.
#if defined(NES_LOGLEVEL_TRACE)
#define NES_COMPILE_TIME_LOG_LEVEL 7
#elif defined(NES_LOGLEVEL_INFO)
#define NES_COMPILE_TIME_LOG_LEVEL 6
#elif defined(NES_LOGLEVEL_DEBUG)
#define NES_COMPILE_TIME_LOG_LEVEL 5
#elif defined(NES_LOGLEVEL_WARN)
#define NES_COMPILE_TIME_LOG_LEVEL 4
#elif defined(NES_LOGLEVEL_ERROR)
#define NES_COMPILE_TIME_LOG_LEVEL 3
#elif defined(NES_LOGLEVEL_FATAL_ERROR)
#define NES_COMPILE_TIME_LOG_LEVEL 2
#elif defined(NES_LOGLEVEL_NONE)
#define NES_LOGGING_NON_LEVEL 1
#endif

constexpr uint64_t getLogLevel(const LogLevel value) { return magic_enum::enum_integer(value); }

class LoggerDetails;
using LoggerDetailsPtr = std::unique_ptr<LoggerDetails>;

class Logger {
  public:
    Logger();
    static Logger* getInstance();
    static void setupLogging(const std::string& logFileName, LogLevel level);
    void log(const LogLevel& logLevel,
             const std::string& message,
             const std::source_location location = std::source_location::current());
    LogLevel& getCurrentLogLevel();
    void setLogLevel(const LogLevel logLevel);
    void setThreadName(const std::string threadName);

  private:
    LogLevel currentLogLevel;
    LoggerDetailsPtr details;
};



#define NES_LOG(LEVEL, message)                                                                                                  \
    do {                                                                                                                         \
        auto constexpr __level = getLogLevel(LEVEL);                                                                             \
        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= __level) {                                                                   \
            auto __logger = NES::Logger::getInstance();                                                                          \
            if (getLogLevel(__logger->getCurrentLogLevel()) >= __level) {                                                        \
                std::stringbuf __buffer;                                                                                         \
                std::ostream __os(&__buffer);                                                                                    \
                __os << message;                                                                                                 \
                __logger->log(LEVEL, __buffer.str());                                                                            \
            }                                                                                                                    \
        }                                                                                                                        \
    } while (0)

#define NES_TRACE(...) NES_LOG(LogLevel::LOG_TRACE, __VA_ARGS__);
#define NES_INFO(...) NES_LOG(LogLevel::LOG_INFO, __VA_ARGS__);
#define NES_DEBUG(...) NES_LOG(LogLevel::LOG_DEBUG, __VA_ARGS__);
#define NES_WARNING(...) NES_LOG(LogLevel::LOG_WARNING, __VA_ARGS__);
#define NES_ERROR(...) NES_LOG(LogLevel::LOG_ERROR, __VA_ARGS__);
#define NES_FATAL_ERROR(...) NES_LOG(LogLevel::LOG_FATAL_ERROR, __VA_ARGS__);

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

#endif//NES_NES_COMMON_INCLUDE_UTIL_LOGGER_LOGGER_HPP_
