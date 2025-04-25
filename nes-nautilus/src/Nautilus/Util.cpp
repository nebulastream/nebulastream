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

#include <cstdint>
#include <limits>
#include <memory>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Util.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Util
{
void logProxy(const char* message, const LogLevel logLevel)
{
    /// Calling the logger with the message and the log level
    switch (logLevel)
    {
        case LogLevel::LOG_NONE:
            break;
        case LogLevel::LOG_FATAL_ERROR:
            NES_FATAL_ERROR("{}", message);
            break;
        case LogLevel::LOG_ERROR:
            NES_ERROR("{}", message);
            break;
        case LogLevel::LOG_WARNING:
            NES_WARNING("{}", message);
            break;
        case LogLevel::LOG_INFO:
            NES_INFO("{}", message);
            break;
        case LogLevel::LOG_DEBUG:
            NES_DEBUG("{}", message);
            break;
        case LogLevel::LOG_TRACE:
            NES_TRACE("{}", message);
            break;
    }
}

VarVal createNautilusMinValue(const DataType::Type physicalType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8:
            return Util::createNautilusConstValue(std::numeric_limits<int8_t>::min(), physicalType);
        case DataType::Type::INT16:
            return Util::createNautilusConstValue(std::numeric_limits<int16_t>::min(), physicalType);
        case DataType::Type::INT32:
            return Util::createNautilusConstValue(std::numeric_limits<int32_t>::min(), physicalType);
        case DataType::Type::INT64:
            return Util::createNautilusConstValue(std::numeric_limits<int64_t>::min(), physicalType);
        case DataType::Type::UINT8:
            return Util::createNautilusConstValue(std::numeric_limits<uint8_t>::min(), physicalType);
        case DataType::Type::UINT16:
            return Util::createNautilusConstValue(std::numeric_limits<uint16_t>::min(), physicalType);
        case DataType::Type::UINT32:
            return Util::createNautilusConstValue(std::numeric_limits<uint32_t>::min(), physicalType);
        case DataType::Type::UINT64:
            return Util::createNautilusConstValue(std::numeric_limits<uint64_t>::min(), physicalType);
        case DataType::Type::FLOAT32:
            return Util::createNautilusConstValue(std::numeric_limits<float>::min(), physicalType);
        case DataType::Type::FLOAT64:
            return Util::createNautilusConstValue(std::numeric_limits<double>::min(), physicalType);
        default: {
            throw NotImplemented("Physical Type: type {} is currently not implemented", magic_enum::enum_name(physicalType));
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", magic_enum::enum_name(physicalType));
}

VarVal createNautilusMaxValue(const DataType::Type physicalType)
{
    switch (physicalType)
    {
        case DataType::Type::INT8:
            return Util::createNautilusConstValue(std::numeric_limits<int8_t>::max(), physicalType);
        case DataType::Type::INT16:
            return Util::createNautilusConstValue(std::numeric_limits<int16_t>::max(), physicalType);
        case DataType::Type::INT32:
            return Util::createNautilusConstValue(std::numeric_limits<int32_t>::max(), physicalType);
        case DataType::Type::INT64:
            return Util::createNautilusConstValue(std::numeric_limits<int64_t>::max(), physicalType);
        case DataType::Type::UINT8:
            return Util::createNautilusConstValue(std::numeric_limits<uint8_t>::max(), physicalType);
        case DataType::Type::UINT16:
            return Util::createNautilusConstValue(std::numeric_limits<uint16_t>::max(), physicalType);
        case DataType::Type::UINT32:
            return Util::createNautilusConstValue(std::numeric_limits<uint32_t>::max(), physicalType);
        case DataType::Type::UINT64:
            return Util::createNautilusConstValue(std::numeric_limits<uint64_t>::max(), physicalType);
        case DataType::Type::FLOAT32:
            return Util::createNautilusConstValue(std::numeric_limits<float>::max(), physicalType);
        case DataType::Type::FLOAT64:
            return Util::createNautilusConstValue(std::numeric_limits<double>::max(), physicalType);
        default: {
            throw NotImplemented("Physical Type: type {} is currently not implemented", magic_enum::enum_name(physicalType));
        }
    }
}

}
