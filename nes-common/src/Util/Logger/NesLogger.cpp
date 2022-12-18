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
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

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

auto createEmptyLogger() -> spdlog::logger {
    auto sink = std::make_shared<spdlog::sinks::basic_file_sink_st>(DEV_NULL);
    auto logger = spdlog::logger(SPDLOG_NES_LOGGER_NAME, {sink});
    return logger;
}

auto createLogger(std::string loggerPath, LogLevel level) -> spdlog::logger {
    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(loggerPath, true);

    auto spdlogLevel = toSpdlogLevel(level);

    consoleSink->set_level(spdlogLevel);
    consoleSink->set_color_mode(spdlog::color_mode::always);
    fileSink->set_level(spdlogLevel);

    consoleSink->set_pattern(SPDLOG_PATTERN);
    fileSink->set_pattern(SPDLOG_PATTERN);

    auto logger = spdlog::logger(SPDLOG_NES_LOGGER_NAME, {consoleSink, fileSink});

    logger.set_level(spdlogLevel);
#ifdef NES_DEBUG_MODE
    logger.flush_on(spdlog::level::debug);
#else
    logger.flush_on(spdlog::level::err);
#endif

    return logger;
}

void Logger::forceFlush() {
    for (auto& sink : impl.sinks()) {
        sink->flush();
    }
    impl.flush();
}

void Logger::configure(const std::string& logFileName, LogLevel level) {
    auto configuredLogger = detail::createLogger(logFileName, level);
    std::swap(configuredLogger, impl);
    std::swap(level, currentLogLevel);
}

void Logger::changeLogLevel(LogLevel newLevel) {
    auto spdNewLogLevel = detail::toSpdlogLevel(newLevel);
    for (auto& sink : impl.sinks()) {
        sink->set_level(spdNewLogLevel);
    }
    impl.set_level(spdNewLogLevel);
    std::swap(newLevel, currentLogLevel);
}

}// namespace detail
}// namespace NES