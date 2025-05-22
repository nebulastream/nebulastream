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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_enum.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

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


VarVal createNautilusMinValue(const std::shared_ptr<PhysicalType>& physicalType);
VarVal createNautilusMaxValue(const std::shared_ptr<PhysicalType>& physicalType);

template <typename T>
static VarVal createNautilusConstValue(T value, const std::shared_ptr<PhysicalType>& physicalType)
{
    if (const auto basicType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::INT_8:
                return Nautilus::VarVal(nautilus::val<int8_t>(value));
            case BasicPhysicalType::NativeType::INT_16:
                return Nautilus::VarVal(nautilus::val<int16_t>(value));
            case BasicPhysicalType::NativeType::INT_32:
                return Nautilus::VarVal(nautilus::val<int32_t>(value));
            case BasicPhysicalType::NativeType::INT_64:
                return Nautilus::VarVal(nautilus::val<int64_t>(value));
            case BasicPhysicalType::NativeType::UINT_8:
                return Nautilus::VarVal(nautilus::val<uint8_t>(value));
            case BasicPhysicalType::NativeType::UINT_16:
                return Nautilus::VarVal(nautilus::val<uint16_t>(value));
            case BasicPhysicalType::NativeType::UINT_32:
                return Nautilus::VarVal(nautilus::val<uint32_t>(value));
            case BasicPhysicalType::NativeType::UINT_64:
                return Nautilus::VarVal(nautilus::val<uint64_t>(value));
            case BasicPhysicalType::NativeType::FLOAT:
                return Nautilus::VarVal(nautilus::val<float>(value));
            case BasicPhysicalType::NativeType::DOUBLE:
                return Nautilus::VarVal(nautilus::val<double>(value));
            default: {
                throw NotImplemented("Physical Type: type {} is currently not implemented", physicalType->toString());
            }
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", physicalType->toString());
}

}
