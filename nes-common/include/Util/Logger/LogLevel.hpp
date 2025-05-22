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
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace NES
{
/**
 * @brief Indicators for a log level following the priority of log messages.
 * A specific log level contains all log messages of an lower level.
 * For example if LOG_LEVEL is LOG_WARNING, then it also contains LOG_NONE, LOG_FATAL_ERROR, and LOG_ERROR.
 */
enum class LogLevel : uint8_t
{
    /// Indicates that no information will be logged.
    LOG_NONE = 1,
    /// Indicates that only information about fatal errors will be logged.
    LOG_FATAL_ERROR = 2,
    /// Indicates that all kinds of error messages will be logged.
    LOG_ERROR = 3,
    /// Indicates that all warnings and error messages will be logged.
    LOG_WARNING = 4,
    /// Indicates that additional debug messages will be logged.
    LOG_INFO = 5,
    /// Indicates that additional information will be logged.
    LOG_DEBUG = 6,
    /// Indicates that all available information will be logged (can result in massive output).
    LOG_TRACE = 7
};

/**
 * @brief getLogName returns the string representation LogLevel value for a specific LogLevel value.
 * @param value LogLevel
 * @return string of value
 */
std::basic_string_view<char> getLogName(LogLevel value);

/**
 * @brief GetLogLevel returns the integer LogLevel value for an specific LogLevel value.
 * @param value LogLevel
 * @return integer between 1 and 7 to identify the log level.
 */
constexpr uint64_t getLogLevel(const LogLevel value)
{
    return static_cast<std::underlying_type_t<LogLevel>>(value);
}

}
