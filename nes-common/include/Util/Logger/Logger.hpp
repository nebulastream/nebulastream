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
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <iostream>
#include <memory>
#include <sstream>
#define KEEP_OLD_LOGGING
namespace NES {

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

/**
 * @brief LogCaller is our compile-time trampoline to invoke the Logger method for the desired level of logging L
 * @tparam L the level of logging
 */
template<LogLevel L>
struct LogCaller {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&&, fmt::format_string<arguments...>, arguments&&...) {
        // nop
    }
};

template<>
struct LogCaller<LogLevel::LOG_INFO> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().info(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

template<>
struct LogCaller<LogLevel::LOG_TRACE> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().trace(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

template<>
struct LogCaller<LogLevel::LOG_DEBUG> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().debug(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

template<>
struct LogCaller<LogLevel::LOG_ERROR> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().error(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

template<>
struct LogCaller<LogLevel::LOG_FATAL_ERROR> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().fatal(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

template<>
struct LogCaller<LogLevel::LOG_WARNING> {
    template<typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        NES::Logger::getInstance().warn(std::move(loc), std::move(format), std::forward<arguments>(args)...);
    }
};

// in the following code we have the logging macros. the ones that have a 2 at the end use spdlog library natively
// i.e., without using << to concatenate strings
// in the next weeks (starting from 11.12.22) one goal is to migrate all logging calls to use the new macros
// when that is completed the old macros will be removed.

/// @brief this is the old logging macro that is the entry point for logging calls
#define NES_LOG(LEVEL, message)                                                                                                  \
    do {                                                                                                                         \
        auto constexpr __level = NES::getLogLevel(LEVEL);                                                                        \
        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= __level) {                                                                   \
            auto& __logger = NES::Logger::getInstance();                                                                         \
            std::stringbuf __buffer;                                                                                             \
            std::ostream __os(&__buffer);                                                                                        \
            __os << message;                                                                                                     \
            NES::LogCaller<LEVEL>::do_call(spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, "{}", __buffer.str());       \
        }                                                                                                                        \
    } while (0)

/// @brief this is the new logging macro that is the entry point for logging calls
#define NES_LOG2(LEVEL, ...)                                                                                                     \
    do {                                                                                                                         \
        auto constexpr __level = NES::getLogLevel(LEVEL);                                                                        \
        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= __level) {                                                                   \
            auto& __logger = NES::Logger::getInstance();                                                                         \
            NES::LogCaller<LEVEL>::do_call(spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, __VA_ARGS__);                \
        }                                                                                                                        \
    } while (0)

#ifdef KEEP_OLD_LOGGING

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

// Creates a log message with log level trace.
#define NES_TRACE2(...) NES_LOG2(NES::LogLevel::LOG_TRACE, __VA_ARGS__);
// Creates a log message with log level info.
#define NES_INFO2(...) NES_LOG2(NES::LogLevel::LOG_INFO, __VA_ARGS__);
// Creates a log message with log level debug.
#define NES_DEBUG2(...) NES_LOG2(NES::LogLevel::LOG_DEBUG, __VA_ARGS__);
// Creates a log message with log level warning.
#define NES_WARNING2(...) NES_LOG2(NES::LogLevel::LOG_WARNING, __VA_ARGS__);
// Creates a log message with log level error.
#define NES_ERROR2(...) NES_LOG2(NES::LogLevel::LOG_ERROR, __VA_ARGS__);
// Creates a log message with log level fatal error.
#define NES_FATAL_ERROR2(...) NES_LOG2(NES::LogLevel::LOG_FATAL_ERROR, __VA_ARGS__);

#else

// Creates a log message with log level trace.
#define NES_TRACE(...) NES_LOG2(NES::LogLevel::LOG_TRACE, __VA_ARGS__);
// Creates a log message with log level info.
#define NES_INFO(...) NES_LOG2(NES::LogLevel::LOG_INFO, __VA_ARGS__);
// Creates a log message with log level debug.
#define NES_DEBUG(...) NES_LOG2(NES::LogLevel::LOG_DEBUG, __VA_ARGS__);
// Creates a log message with log level warning.
#define NES_WARNING(...) NES_LOG2(NES::LogLevel::LOG_WARNING, __VA_ARGS__);
// Creates a log message with log level error.
#define NES_ERROR(...) NES_LOG2(NES::LogLevel::LOG_ERROR, __VA_ARGS__);
// Creates a log message with log level fatal error.
#define NES_FATAL_ERROR(...) NES_LOG2(NES::LogLevel::LOG_FATAL_ERROR, __VA_ARGS__);

#endif

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
