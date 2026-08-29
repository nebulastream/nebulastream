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

#include <atomic>
#include <string>
#include <utility>
#include <Util/Logger/LogLevel.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <spdlog/fwd.h>
#include <spdlog/logger.h>
#include <spdlog/mdc.h>

namespace spdlog::details
{
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
    std::unique_ptr<spdlog::details::periodic_worker> flusher{nullptr};
};

/// Process-wide cached copy of the current log level, published by Logger::changeLogLevel and read on hot
/// paths (LogContext) with a single relaxed atomic load -- avoids copying the logger singleton's shared_ptr
/// per task just to gate diagnostic-context work. Defaults to LOG_INFO until the logger is configured.
inline std::atomic<LogLevel>& cachedLogLevel() noexcept
{
    static std::atomic<LogLevel> level{LogLevel::LOG_INFO};
    return level;
}
}

namespace Logger
{
void setupLogging(const std::string& logFileName, LogLevel level, bool useStdout = true);

std::shared_ptr<detail::Logger> getInstance();
}

/// Pushes a diagnostic context (spdlog MDC) so log lines emitted within its scope carry it. This is
/// constructed on hot paths (e.g. one per task in the query engine), so the value is formatted LAZILY and
/// only when a level that could actually consume it is active: at DEBUG/TRACE we populate the MDC, otherwise
/// we skip the format + the two thread-local map operations entirely. Formatting the arguments (e.g. a query
/// UUID) is otherwise pure per-task overhead at INFO/WARN/ERROR, where the compiled-out ENGINE_LOG_DEBUG
/// lines that would read the context never fire. Pass a format string + args (NOT a pre-formatted string) so
/// the fmt::format is deferred behind the gate.
struct LogContext
{
    std::string context;
    bool active;

    template <typename... Args>
    explicit LogContext(std::string context, fmt::format_string<Args...> format, Args&&... args)
        : context(std::move(context)), active(detail::cachedLogLevel().load(std::memory_order_relaxed) >= LogLevel::LOG_DEBUG)
    {
        if (active)
        {
            spdlog::mdc::put(this->context, fmt::format(format, std::forward<Args>(args)...));
        }
    }

    ~LogContext()
    {
        if (active)
        {
            spdlog::mdc::remove(context);
        }
    }
};

}
