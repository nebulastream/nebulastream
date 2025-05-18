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

#pragma once
#include <memory>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

/// In the following we define the NES_COMPILE_TIME_LOG_LEVEL macro.
/// This macro indicates the log level, which was chosen at compilation time and enables the complete
/// elimination of log messages.
#if defined(NES_LOGLEVEL_TRACE)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 7;
#elif defined(NES_LOGLEVEL_DEBUG)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 6;
#elif defined(NES_LOGLEVEL_INFO)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 5;
#elif defined(NES_LOGLEVEL_WARN)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 4;
#elif defined(NES_LOGLEVEL_ERROR)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 3;
#elif defined(NES_LOGLEVEL_FATAL_ERROR)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 2;
#elif defined(NES_LOGLEVEL_NONE)
constexpr uint64_t NES_COMPILE_TIME_LOG_LEVEL = 1;
#else
    #error no loglevel defined
#endif


namespace NES
{

/// @brief LogCaller is our compile-time trampoline to invoke the Logger method for the desired level of logging L
/// @tparam L the level of logging
template <LogLevel L>
struct LogCaller
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&&, fmt::format_string<arguments...>, arguments&&...)
    {
        /// nop
    }
};

template <>
struct LogCaller<LogLevel::LOG_INFO>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->info(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

template <>
struct LogCaller<LogLevel::LOG_TRACE>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->trace(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

template <>
struct LogCaller<LogLevel::LOG_DEBUG>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->debug(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

template <>
struct LogCaller<LogLevel::LOG_ERROR>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->error(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

template <>
struct LogCaller<LogLevel::LOG_FATAL_ERROR>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->fatal(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

template <>
struct LogCaller<LogLevel::LOG_WARNING>
{
    template <typename... arguments>
    constexpr static void do_call(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        if (auto instance = NES::Logger::getInstance())
        {
            instance->warn(std::move(loc), std::move(format), std::forward<arguments>(args)...);
        }
    }
};

/// Macro to suppress unused warnings
#define SUPPRESS_UNUSED_WARNING(...) \
    do \
    { \
        [](auto&&... args) { ((void)args, ...); }(__VA_ARGS__); \
    } while (0)


/// @brief this is the new logging macro that is the entry point for logging calls
#define NES_LOG(LEVEL, ...) \
    do \
    { \
        auto constexpr __level = NES::getLogLevel(LEVEL); \
        if constexpr (NES_COMPILE_TIME_LOG_LEVEL >= __level) \
        { \
            NES::LogCaller<LEVEL>::do_call(spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, __VA_ARGS__); \
        } \
        else \
        { \
            SUPPRESS_UNUSED_WARNING(__VA_ARGS__); \
        } \
\
    } while (0)

/// Creates a log message with log level trace.
#define NES_TRACE(...) NES_LOG(NES::LogLevel::LOG_TRACE, __VA_ARGS__);
/// Creates a log message with log level info.
#define NES_INFO(...) NES_LOG(NES::LogLevel::LOG_INFO, __VA_ARGS__);
/// Creates a log message with log level debug.
#define NES_DEBUG(...) NES_LOG(NES::LogLevel::LOG_DEBUG, __VA_ARGS__);
/// Creates a log message with log level warning.
#define NES_WARNING(...) NES_LOG(NES::LogLevel::LOG_WARNING, __VA_ARGS__);
/// Creates a log message with log level error.
#define NES_ERROR(...) NES_LOG(NES::LogLevel::LOG_ERROR, __VA_ARGS__);
/// Creates a log message with log level fatal error.
#define NES_FATAL_ERROR(...) NES_LOG(NES::LogLevel::LOG_FATAL_ERROR, __VA_ARGS__);
}
