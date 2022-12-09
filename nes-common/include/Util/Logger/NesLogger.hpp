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

#ifndef NES_COMMON_INCLUDE_UTIL_LOGGER_NESLOGGER_HPP_
#define NES_COMMON_INCLUDE_UTIL_LOGGER_NESLOGGER_HPP_

#include <Util/Logger/LogLevel.hpp>
#include <fmt/core.h>
#include <spdlog/spdlog.h>

namespace NES {

namespace detail {
spdlog::logger createLogger();
spdlog::logger createEmptyLogger();
}// namespace detail

namespace Logger {
void setupLogging(const std::string& logFileName, LogLevel level);
}

class NesLogger {
  private:
    using format_string_type = fmt::basic_string_view<char>;

  public:
    void configure(const std::string& logFileName, LogLevel level);

    ~NesLogger() {
        forceFlush();
        spdlog::shutdown();
    }

    NesLogger() : impl(detail::createEmptyLogger()) {}

    NesLogger(const NesLogger&) = delete;

    void operator=(const NesLogger&) = delete;

  private:
  public:
    template<typename... arguments>
    constexpr inline void trace(spdlog::source_loc&& loc, fmt::format_string<arguments...>&& format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::trace, std::move(format), std::forward<arguments>(args)...);
    }

    template<typename... arguments>
    constexpr inline void warn(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::warn, std::move(format), std::forward<arguments>(args)...);
    }

    template<typename... arguments>
    constexpr inline void fatal(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::critical, std::move(format), std::forward<arguments>(args)...);
    }

    template<typename... arguments>
    constexpr inline void info(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::info, std::move(format), std::forward<arguments>(args)...);
    }

    template<typename... arguments>
    constexpr inline void debug(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::debug, std::move(format), std::forward<arguments>(args)...);
    }

    template<typename... arguments>
    constexpr inline void error(spdlog::source_loc&& loc, fmt::format_string<arguments...> format, arguments&&... args) {
        impl.log(std::move(loc), spdlog::level::err, std::move(format), std::forward<arguments>(args)...);
    }

    void flush() { impl.flush(); }

    void forceFlush();

    inline LogLevel getCurrentLogLevel() const noexcept { return currentLogLevel; }

     void changeLogLevel(LogLevel newLevel);

  public:
    static NesLogger& getInstance();// singleton is ok here

  private:
    spdlog::logger impl;
    LogLevel currentLogLevel = LogLevel::LOG_INFO;
};
}// namespace NES
#endif//NES_COMMON_INCLUDE_UTIL_LOGGER_NESLOGGER_HPP_
