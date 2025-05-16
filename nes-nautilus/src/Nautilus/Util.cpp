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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Util.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

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

VarVal createNautilusMinValue(const PhysicalType& physicalType)
{
    if (const auto* basicType = dynamic_cast<const BasicPhysicalType*>(&physicalType))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::INT_8:
                return Util::createNautilusConstValue(std::numeric_limits<int8_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_16:
                return Util::createNautilusConstValue(std::numeric_limits<int16_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_32:
                return Util::createNautilusConstValue(std::numeric_limits<int32_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_64:
                return Util::createNautilusConstValue(std::numeric_limits<int64_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_8:
                return Util::createNautilusConstValue(std::numeric_limits<uint8_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_16:
                return Util::createNautilusConstValue(std::numeric_limits<uint16_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_32:
                return Util::createNautilusConstValue(std::numeric_limits<uint32_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_64:
                return Util::createNautilusConstValue(std::numeric_limits<uint64_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::FLOAT:
                return Util::createNautilusConstValue(std::numeric_limits<float>::min(), physicalType);
            case BasicPhysicalType::NativeType::DOUBLE:
                return Util::createNautilusConstValue(std::numeric_limits<double>::min(), physicalType);
            default: {
                throw NotImplemented("Physical Type: type {} is currently not implemented", physicalType.toString());
            }
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", physicalType.toString());
}

VarVal createNautilusMaxValue(const PhysicalType& physicalType)
{
    if (const auto* basicType = dynamic_cast<const BasicPhysicalType*>(&physicalType))
    {
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::INT_8:
                return Util::createNautilusConstValue(std::numeric_limits<int8_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_16:
                return Util::createNautilusConstValue(std::numeric_limits<int16_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_32:
                return Util::createNautilusConstValue(std::numeric_limits<int32_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_64:
                return Util::createNautilusConstValue(std::numeric_limits<int64_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_8:
                return Util::createNautilusConstValue(std::numeric_limits<uint8_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_16:
                return Util::createNautilusConstValue(std::numeric_limits<uint16_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_32:
                return Util::createNautilusConstValue(std::numeric_limits<uint32_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_64:
                return Util::createNautilusConstValue(std::numeric_limits<uint64_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::FLOAT:
                return Util::createNautilusConstValue(std::numeric_limits<float>::max(), physicalType);
            case BasicPhysicalType::NativeType::DOUBLE:
                return Util::createNautilusConstValue(std::numeric_limits<double>::max(), physicalType);
            default: {
                throw NotImplemented("Physical Type: type {} is currently not implemented", physicalType.toString());
            }
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", physicalType.toString());
}

}
