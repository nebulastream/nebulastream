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
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>

namespace NES::Nautilus::Util
{


void logProxy(const char* message, const LogLevel logLevel);

/// Allows to use our general logging calls from our nautilus-runtime
#define NES_TRACE_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_TRACE); \
        nautilus::invoke(NES::Nautilus::Util::logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_DEBUG_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_DEBUG); \
        nautilus::invoke(NES::Nautilus::Util::logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_INFO_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_INFO); \
        nautilus::invoke(NES::Nautilus::Util::logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_WARNING_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_WARNING); \
        nautilus::invoke(NES::Nautilus::Util::logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_ERROR_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_ERROR); \
        nautilus::invoke(NES::Nautilus::Util::logProxy, ss.str().c_str(), logLevel); \
    } while (0)


}
