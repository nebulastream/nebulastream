#include <Util/Logger/Logger.hpp>
#include <Util/Logger/NesLogger.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace NES {

namespace Logger {
void setupLogging(const std::string& logFileName, LogLevel level) { NesLogger::getInstance().configure(logFileName, level); }
}// namespace Logger

namespace detail {

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
    auto sink = std::make_shared<spdlog::sinks::basic_file_sink_st>("/dev/null");
    spdlog::logger logger = spdlog::logger("nes_dummy", {sink});
    return logger;
}

auto createLogger(std::string loggerPath, LogLevel level) -> spdlog::logger {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(loggerPath, true);

    auto spdlogLevel = toSpdlogLevel(level);

    console_sink->set_level(spdlogLevel);
    console_sink->set_color_mode(spdlog::color_mode::always);
    file_sink->set_level(spdlogLevel);

    console_sink->set_pattern("%^[%H:%M:%S.%f] [%L] [thread %t] [%s:%# %!] %v%$");
    file_sink->set_pattern("%^[%H:%M:%S.%f] [%L] [thread %t] [%s:%# %!] %v%$");

    spdlog::logger logger = spdlog::logger("nes_logger", {console_sink, file_sink});

    logger.set_level(spdlogLevel);
#ifdef NES_DEBUG_MODE
    logger.flush_on(spdlog::level::debug);
#else
    logger.flush_on(spdlog::level::err);
#endif

    return logger;
}
}// namespace detail

void NesLogger::forceFlush() {
    for (auto& sink : impl.sinks()) {
        sink->flush();
    }
    impl.flush();
}

void NesLogger::configure(const std::string& logFileName, LogLevel level) {
    auto configuredLogger = detail::createLogger(logFileName, level);
    std::swap(configuredLogger, impl);
    std::swap(level, currentLogLevel);
}

void NesLogger::changeLogLevel(LogLevel newLevel) {
    auto spdNewLogLevel = detail::toSpdlogLevel(newLevel);
    for (auto& sink : impl.sinks()) {
        sink->set_level(spdNewLogLevel);
    }
    impl.set_level(spdNewLogLevel);
}

NesLogger& NesLogger::getInstance() {
    static NesLogger singleton;

    return singleton;
}
}// namespace NES