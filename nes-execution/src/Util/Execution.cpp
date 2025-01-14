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

#include <tuple>

#include <API/AttributeField.hpp>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Execution.hpp>
#include <nautilus/std/string.h>
#include <magic_enum.hpp>


namespace NES::QueryCompilation::Util
{
std::tuple<uint64_t, uint64_t, Runtime::Execution::Operators::TimeFunctionPtr>
getWindowingParameters(Windowing::TimeBasedWindowType& windowType)
{
    const auto& windowSize = windowType.getSize().getTime();
    const auto& windowSlide = windowType.getSlide().getTime();
    const auto type = windowType.getTimeCharacteristic()->getType();

    switch (type)
    {
        case Windowing::TimeCharacteristic::Type::IngestionTime: {
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
            return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
        }
        case Windowing::TimeCharacteristic::Type::EventTime: {
            const auto& timeStampFieldName = windowType.getTimeCharacteristic()->getField()->getName();
            auto timeStampFieldRecord = std::make_unique<Runtime::Execution::Functions::ExecutableFunctionReadField>(timeStampFieldName);
            auto timeFunction = std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                std::move(timeStampFieldRecord), windowType.getTimeCharacteristic()->getTimeUnit());
            return std::make_tuple(windowSize, windowSlide, std::move(timeFunction));
        }
    }
}
}

namespace NES::Runtime::Execution
{
Nautilus::VarVal Util::createMinValue(const PhysicalTypePtr& physicalType)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(physicalType))
    {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::INT_8:
                return Util::createConstValue(std::numeric_limits<int8_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_16:
                return Util::createConstValue(std::numeric_limits<int16_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_32:
                return Util::createConstValue(std::numeric_limits<int32_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::INT_64:
                return Util::createConstValue(std::numeric_limits<int64_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_8:
                return Util::createConstValue(std::numeric_limits<uint8_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_16:
                return Util::createConstValue(std::numeric_limits<uint16_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_32:
                return Util::createConstValue(std::numeric_limits<uint32_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::UINT_64:
                return Util::createConstValue(std::numeric_limits<uint64_t>::min(), physicalType);
            case BasicPhysicalType::NativeType::FLOAT:
                return Util::createConstValue(std::numeric_limits<float>::min(), physicalType);
            case BasicPhysicalType::NativeType::DOUBLE:
                return Util::createConstValue(std::numeric_limits<double>::min(), physicalType);
            default: {
                throw NotImplemented("Physical Type: type {} is currently not implemented", physicalType->toString());
            }
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", physicalType->toString());
}

Nautilus::VarVal Util::createMaxValue(const PhysicalTypePtr& physicalType)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(physicalType))
    {
        auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
        switch (basicType->nativeType)
        {
            case BasicPhysicalType::NativeType::INT_8:
                return Util::createConstValue(std::numeric_limits<int8_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_16:
                return Util::createConstValue(std::numeric_limits<int16_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_32:
                return Util::createConstValue(std::numeric_limits<int32_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::INT_64:
                return Util::createConstValue(std::numeric_limits<int64_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_8:
                return Util::createConstValue(std::numeric_limits<uint8_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_16:
                return Util::createConstValue(std::numeric_limits<uint16_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_32:
                return Util::createConstValue(std::numeric_limits<uint32_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::UINT_64:
                return Util::createConstValue(std::numeric_limits<uint64_t>::max(), physicalType);
            case BasicPhysicalType::NativeType::FLOAT:
                return Util::createConstValue(std::numeric_limits<float>::max(), physicalType);
            case BasicPhysicalType::NativeType::DOUBLE:
                return Util::createConstValue(std::numeric_limits<double>::max(), physicalType);
            default: {
                throw NotImplemented("Physical Type: type {} is currently not implemented", physicalType->toString());
            }
        }
    }
    throw NotImplemented("Physical Type: type {} is not a BasicPhysicalType", physicalType->toString());
}

}
