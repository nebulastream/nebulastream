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

#include <string>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/details/periodic_worker.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace NES
{

namespace detail
{

static constexpr auto SPDLOG_NES_LOGGER_NAME = "nes_logger";
static constexpr auto DEV_NULL = "/dev/null";
static constexpr auto SPDLOG_PATTERN = "%^[%H:%M:%S.%f] [%L] [thread %t] [%s:%#] [%!] %v%$";

auto toSpdlogLevel(const LogLevel level)
{
    auto spdlogLevel = spdlog::level::info;
    switch (level)
    {
        case LogLevel::LOG_DEBUG: {
            spdlogLevel = spdlog::level::debug;
            break;
        }
        case LogLevel::LOG_INFO: {
            spdlogLevel = spdlog::level::info;
            break;
        }
        case LogLevel::LOG_TRACE: {
            spdlogLevel = spdlog::level::trace;
            break;
        }
        case LogLevel::LOG_WARNING: {
            spdlogLevel = spdlog::level::warn;
            break;
        }
        case LogLevel::LOG_ERROR: {
            spdlogLevel = spdlog::level::err;
            break;
        }
        case LogLevel::LOG_FATAL_ERROR: {
            spdlogLevel = spdlog::level::critical;
            break;
        }
        case LogLevel::LOG_NONE: {
            spdlogLevel = spdlog::level::off;
            break;
        }
        default: {
            break;
        }
    }
    return spdlogLevel;
}

auto createEmptyLogger() -> std::shared_ptr<spdlog::logger>
{
    return std::make_shared<spdlog::logger>("null", std::make_shared<spdlog::sinks::basic_file_sink_st>(DEV_NULL));
}

Logger::Logger(const std::string& logFileName, const LogLevel level, const bool useStdout)
{
    static constexpr auto QUEUE_SIZE = 8 * 1024;
    static constexpr auto THREADS = 1;
    loggerThreadPool = std::make_shared<spdlog::details::thread_pool>(QUEUE_SIZE, THREADS);
    auto spdlogLevel = toSpdlogLevel(level);

    std::vector<spdlog::sink_ptr> sinks;
    if (useStdout)
    {
        auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        consoleSink->set_level(spdlogLevel);
        consoleSink->set_color_mode(spdlog::color_mode::always);
        consoleSink->set_pattern(SPDLOG_PATTERN);
        sinks.push_back(consoleSink);
    }

    auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logFileName, true);
    fileSink->set_level(spdlogLevel);
    fileSink->set_pattern(SPDLOG_PATTERN);
    sinks.push_back(fileSink);

    impl = std::make_shared<spdlog::async_logger>(
        SPDLOG_NES_LOGGER_NAME, sinks.begin(), sinks.end(), loggerThreadPool, spdlog::async_overflow_policy::block);

    impl->flush_on(spdlog::level::debug);

    changeLogLevel(level);

    flusher = std::make_unique<spdlog::details::periodic_worker>([this]() { impl->flush(); }, std::chrono::seconds(1));
}

Logger::Logger() : impl(detail::createEmptyLogger())
{
}

Logger::~Logger()
{
    shutdown();
}

/// As we are using the async logger, this flushes async and does not block!
/// shutdown the logger to ensure all msgs are sent before calling e.g. abort()
void Logger::forceFlush()
{
    if (impl)
    {
        for (auto& sink : impl->sinks())
        {
            sink->flush();
        }
        impl->flush();
    }
}

void Logger::shutdown()
{
    bool expected = false;
    if (isShutdown.compare_exchange_strong(expected, true))
    {
        flusher.reset();
        impl.reset();
        loggerThreadPool.reset();
    }
}

void Logger::changeLogLevel(LogLevel newLevel)
{
    auto spdNewLogLevel = detail::toSpdlogLevel(newLevel);
    for (auto& sink : impl->sinks())
    {
        sink->set_level(spdNewLogLevel);
    }
    impl->set_level(spdNewLogLevel);
    std::swap(newLevel, currentLogLevel);
}

struct LoggerHolder
{
    static std::shared_ptr<Logger> singleton;

    ~LoggerHolder()
    {
        singleton.reset();
        /// Not sure why, but we have to disable the call to spdlog::shutdown() here. Otherwise, the system tests will not shutdown properly.
        /// The actual error happens in std::__hash_table::__deallocate_node() line 1109.
        /// TODO #348: Investigate why the call to spdlog::shutdown() causes the system tests to fail.
        /// spdlog::shutdown();
    }
};

std::shared_ptr<Logger> LoggerHolder::singleton = nullptr;

}

namespace Logger
{

static detail::LoggerHolder helper;

void setupLogging(const std::string& logFileName, LogLevel level, bool useStdout)
{
    auto newLogger = std::make_shared<detail::Logger>(logFileName, level, useStdout);
    std::swap(detail::LoggerHolder::singleton, newLogger);
}

std::shared_ptr<detail::Logger> getInstance()
{
    return detail::LoggerHolder::singleton;
}

}

}
