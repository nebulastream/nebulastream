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


#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace NES {

namespace detail {
spdlog::logger create_logger();
}  // namespace detail

class logger {
  private:
    using format_string_type = fmt::basic_string_view<char>;

  private:
    logger() : _logger(detail::create_logger()) {}

    ~logger() {
        _logger.flush();
        spdlog::shutdown();
    }

  public:
    logger(const logger&) = delete;

    void operator=(const logger&) = delete;

  private:
  public:
    template <typename... arguments>
    inline void trace(format_string_type format, arguments&&... args) {
        _logger.trace(format, std::forward<arguments>(args)...);
    }

    template <typename... arguments>
    inline void info(format_string_type format, arguments&&... args) {
        _logger.info(format, std::forward<arguments>(args)...);
    }

    template <typename... arguments>
    inline void debug(format_string_type format, arguments&&... args) {
        _logger.debug(format, std::forward<arguments>(args)...);
    }

    template <typename... arguments>
    inline void error(format_string_type format, arguments&&... args) {
        _logger.error(format, std::forward<arguments>(args)...);
    }

    void flush() { _logger.flush(); }

    void force_flush() {
        for (auto& sink : _logger.sinks()) {
            sink->flush();
        }
        _logger.flush();
    }

  public:
    static logger& instance();

  private:
    spdlog::logger _logger;
};


#endif//NES_COMMON_INCLUDE_UTIL_LOGGER_NESLOGGER_HPP_
