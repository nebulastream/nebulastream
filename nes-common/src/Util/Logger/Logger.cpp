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
#include <Util/Logger/LoggerDetails.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES {

Logger* Logger::getInstance() {
    static Logger instance = Logger();
    return &instance;
}

Logger::Logger() : details(std::make_unique<detail::LoggerDetails>()) {}

LogLevel& Logger::getCurrentLogLevel() { return currentLogLevel; }

void Logger::setLogLevel(const LogLevel logLevel) {
    if (getLogLevel(logLevel) > NES_COMPILE_TIME_LOG_LEVEL) {
        NES_FATAL_ERROR("The log level " << magic_enum::enum_name(logLevel)
                                         << " is not available as NebulaStream was compiled with loglevel: "
                                         << magic_enum::enum_name(magic_enum::enum_value<LogLevel>(NES_COMPILE_TIME_LOG_LEVEL)));
    }
    this->currentLogLevel = logLevel;
    this->details->setLogLevel(logLevel);
};

void Logger::log(const LogLevel& logLevel, const std::string& message, const std::source_location location) {
    details->log(logLevel, message, location);
}

void Logger::setupLogging(const std::string& logFileName, LogLevel level) {
    auto instance = getInstance();
    instance->details->setupLogging(logFileName);
    instance->setLogLevel(level);
}
void Logger::setThreadName(const std::string threadName) { details->setThreadName(threadName); }

}// namespace NES