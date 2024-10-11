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

#include <Util/Logger/LogLevel.hpp>
#include <fmt/core.h>
#include <spdlog/fwd.h>
#include <spdlog/logger.h>

namespace spdlog::details
{
class thread_pool;
class periodic_worker;
}

namespace NES
{

namespace detail
{
/// Creates an empty logger that writes to /dev/null
std::shared_ptr<spdlog::logger> createEmptyLogger();
}

namespace detail
{
class Logger
{
public:
    explicit Logger(const std::string& logFileName, LogLevel level, bool useStdout = true);
    ~Logger();
    Logger();

    Logger(const Logger&) = delete;
    void operator=(const Logger&) = delete;

    void shutdown();

    /// Logs a tracing message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void trace(spdlog::source_loc&& loc, fmt::format_string<arguments...>&& format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::trace, std::move(format), std::forward<arguments>(args)...);
    }

    /// Logs a warning message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void warn(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::warn, std::move(format), std::forward<arguments>(args)...);
    }

    /// Logs a fatal error message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void fatal(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::critical, std::move(format), std::forward<arguments>(args)...);
    }

    /// Logs an info message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void info(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::info, std::move(format), std::forward<arguments>(args)...);
    }

    /// Logs a debug message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void debug(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::debug, std::move(format), std::forward<arguments>(args)...);
    }

    /// Logs an error message using a format, a source location, and a set of arguments to display
    template <typename... arguments>
    constexpr inline void error(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args)
    {
        impl->log(std::move(loc), spdlog::level::err, std::move(format), std::forward<arguments>(args)...);
    }

    /// flushes the current log to filesystem
    void flush() { impl->flush(); }

    /// forcefully flushes the current log to filesystem
    void forceFlush();

    inline LogLevel getCurrentLogLevel() const noexcept { return currentLogLevel; }

    void changeLogLevel(LogLevel newLevel);

private:
    std::shared_ptr<spdlog::logger> impl{nullptr};
    LogLevel currentLogLevel = LogLevel::LOG_INFO;
    std::atomic<bool> isShutdown{false};
    std::shared_ptr<spdlog::details::thread_pool> loggerThreadPool{nullptr};
    std::unique_ptr<spdlog::details::periodic_worker> flusher{nullptr};
};
}

namespace Logger
{
void setupLogging(const std::string& logFileName, LogLevel level, bool useStdout = true);

std::shared_ptr<detail::Logger> getInstance();
}

}
