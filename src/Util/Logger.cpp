/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Util/Logger.hpp>

namespace NES {

    log4cxx::LoggerPtr& NESLogger::getInstance() {
        static log4cxx::LoggerPtr instance = log4cxx::Logger::getLogger("NES");
        return instance;
    }
    NESLogger::NESLogger() {};

    std::string getDebugLevelAsString(DebugLevel level) {
        switch (level) {
            case LOG_NONE: return "LOG_NONE";
            case LOG_WARNING: return "LOG_WARNING";
            case LOG_ERROR: return "LOG_ERROR";
            case LOG_FATAL: return "LOG_FATAL";
            case LOG_DEBUG: return "LOG_DEBUG";
            case LOG_INFO: return "LOG_INFO";
            case LOG_TRACE: return "LOG_TRACE";
            default: return "UNKNOWN";
        }
    }

    DebugLevel getDebugLevelFromString(const std::string& level) {
        if (level == "LOG_NONE") {
            return LOG_NONE;
        }
        if (level == "LOG_WARNING") {
            return LOG_WARNING;
        } else if (level == "LOG_ERROR") {
            return LOG_ERROR;
        } else if (level == "LOG_FATAL") {
            return LOG_FATAL;
        } else if (level == "LOG_DEBUG") {
            return LOG_DEBUG;
        } else if (level == "LOG_INFO") {
            return LOG_INFO;
        } else if (level == "LOG_TRACE") {
            return LOG_TRACE;
        } else {
            throw std::runtime_error("Logger: Debug level unknown: " + level);
        }
    }

    void setLogLevel(DebugLevel level) {
        // set log level
#ifdef NES_LOGGING_NO_LEVEL
        NESLogger::getInstance()->setLevel(log4cxx::Level::getOff());
        ((void) level);
#else
        // set log level
        switch (level) {
            case LOG_NONE: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getOff());
                break;
            }
            case LOG_ERROR: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getError());
                break;
            }
            case LOG_FATAL: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getFatal());
                break;
            }
            case LOG_WARNING: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getWarn());
                break;
            }
            case LOG_DEBUG: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getDebug());
                break;
            }
            case LOG_INFO: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getInfo());
                break;
            }
            case LOG_TRACE: {
                NESLogger::getInstance()->setLevel(log4cxx::Level::getTrace());
                break;
            }
            default: {
                NES_FATAL_ERROR("setupLogging: log level not supported " << getDebugLevelAsString(level));
            }
        }
#endif
    }

    void setupLogging(const std::string& logFileName, DebugLevel level) {
        std::cout << "Logger: SETUP_LOGGING" << std::endl;
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
                new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c: %l [%M] %X{threadName} [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, logFileName);
        log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));
        setLogLevel(level);
        NESLogger::getInstance()->addAppender(file);
        NESLogger::getInstance()->addAppender(console);
    }

}// namespace NES
