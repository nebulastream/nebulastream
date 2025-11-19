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
#include <memory>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>

namespace NES
{
void logProxy(const char* message, const LogLevel logLevel);

/// Allows to use our general logging calls from our nautilus-runtime
#define NES_TRACE_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_TRACE); \
        nautilus::invoke(logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_DEBUG_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_DEBUG); \
        nautilus::invoke(logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_INFO_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_INFO); \
        nautilus::invoke(logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_WARNING_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_WARNING); \
        nautilus::invoke(logProxy, ss.str().c_str(), logLevel); \
    } while (0)

#define NES_ERROR_EXEC(TEXT) \
    do \
    { \
        nautilus::stringstream ss; \
        ss << TEXT; \
        const nautilus::val<LogLevel> logLevel = (LogLevel::LOG_ERROR); \
        nautilus::invoke(logProxy, ss.str().c_str(), logLevel); \
    } while (0)


VarVal createNautilusMinValue(DataType::Type physicalType);
VarVal createNautilusMaxValue(DataType::Type physicalType);

template <typename T>
static VarVal createNautilusConstValue(T value, DataType::Type physicalType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8:
            return VarVal(nautilus::val<int8_t>(value));
        case DataType::Type::INT16:
            return VarVal(nautilus::val<int16_t>(value));
        case DataType::Type::INT32:
            return VarVal(nautilus::val<int32_t>(value));
        case DataType::Type::INT64:
            return VarVal(nautilus::val<int64_t>(value));
        case DataType::Type::UINT8:
            return VarVal(nautilus::val<uint8_t>(value));
        case DataType::Type::UINT16:
            return VarVal(nautilus::val<uint16_t>(value));
        case DataType::Type::UINT32:
            return VarVal(nautilus::val<uint32_t>(value));
        case DataType::Type::UINT64:
            return VarVal(nautilus::val<uint64_t>(value));
        case DataType::Type::FLOAT32:
            return VarVal(nautilus::val<float>(value));
        case DataType::Type::FLOAT64:
            return VarVal(nautilus::val<double>(value));
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
        case DataType::Type::UNDEFINED:
            throw UnknownDataType("Not supporting reading {} data type from memory.", magic_enum::enum_name(physicalType));
    }
    std::unreachable();
}

}
