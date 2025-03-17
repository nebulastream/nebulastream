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
#include <DataTypes/DataTypeUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Util.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
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
    return DataTypeUtil::dispatchByNumericalType(
        physicalType, [physicalType]<typename T>() { return Util::createNautilusConstValue(std::numeric_limits<T>::min(), physicalType); });
}

VarVal createNautilusMaxValue(const DataType::Type physicalType)
{
    return DataTypeUtil::dispatchByNumericalType(
        physicalType, [physicalType]<typename T>() { return Util::createNautilusConstValue(std::numeric_limits<T>::max(), physicalType); });
}

}
