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

#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <spdlog/async.h>
#include <spdlog/async_logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace detail {
struct LoggerShutdownHelper {
    ~LoggerShutdownHelper() { NES::Logger::getInstance().shutdown(); }
};
static LoggerShutdownHelper helper;
}// namespace detail

namespace NES {

namespace Logger {

void setupLogging(const std::string& logFileName, LogLevel level) { Logger::getInstance().configure(logFileName, level); }

detail::Logger& getInstance() {
    static detail::Logger singleton;

    return singleton;
}
}// namespace Logger

namespace detail {
static constexpr auto SPDLOG_NES_LOGGER_NAME = "nes_logger";
static constexpr auto DEV_NULL = "/dev/null";
static constexpr auto SPDLOG_PATTERN = "%^[%H:%M:%S.%f] [%L] [thread %t] [%s:%#] [%!] %v%$";

auto toSpdlogLevel(LogLevel level) {
    auto spdlogLevel = spdlog::level::info;
    switch (level) {
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
        default: {
            break;
        }
    }
    return spdlogLevel;
}

auto createEmptyLogger() -> std::shared_ptr<spdlog::logger> { return nullptr; }

auto createLogger(std::string loggerPath, LogLevel level) -> std::shared_ptr<spdlog::logger> {
    static constexpr auto QUEUE_SIZE = 8 * 1024;
    static constexpr auto THREADS = 1;
    spdlog::init_thread_pool(QUEUE_SIZE, THREADS);

    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(loggerPath, true);

    auto spdlogLevel = toSpdlogLevel(level);

    consoleSink->set_level(spdlogLevel);
    consoleSink->set_color_mode(spdlog::color_mode::always);
    fileSink->set_level(spdlogLevel);

    consoleSink->set_pattern(SPDLOG_PATTERN);
    fileSink->set_pattern(SPDLOG_PATTERN);

    std::vector<spdlog::sink_ptr> sinks = {consoleSink, fileSink};

    auto logger = std::make_shared<spdlog::async_logger>(SPDLOG_NES_LOGGER_NAME,
                                                         sinks.begin(),
                                                         sinks.end(),
                                                         spdlog::thread_pool(),
                                                         spdlog::async_overflow_policy::block);
    
    logger->set_level(spdlogLevel);
    logger->flush_on(spdlog::level::debug);

    return logger;
}

Logger::~Logger() {
    //shutdown();
}

void Logger::forceFlush() {
    for (auto& sink : impl->sinks()) {
        sink->flush();
    }
    impl->flush();
}

void Logger::shutdown() {
    bool expected = false;
    if (isShutdown.compare_exchange_strong(expected, true)) {
        spdlog::shutdown();
    }
}

void Logger::configure(const std::string& logFileName, LogLevel level) {
    auto configuredLogger = detail::createLogger(logFileName, level);
    std::swap(configuredLogger, impl);
    std::swap(level, currentLogLevel);
}

void Logger::changeLogLevel(LogLevel newLevel) {
    auto spdNewLogLevel = detail::toSpdlogLevel(newLevel);
    for (auto& sink : impl->sinks()) {
        sink->set_level(spdNewLogLevel);
    }
    impl->set_level(spdNewLogLevel);
    std::swap(newLevel, currentLogLevel);
}
}// namespace detail
}// namespace NES