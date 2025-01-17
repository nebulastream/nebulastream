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
#include <sstream>
#include <string>
#include <tuple>
#include <utility>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Time/Timestamp.hpp>
#include <Types/TimeBasedWindowType.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>

#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>


namespace NES::QueryCompilation
{
enum class JoinBuildSideType : uint8_t
{
    Right,
    Left
};
}


namespace NES::Runtime::Execution
{

/// Stores the window start and window end field names
struct WindowMetaData
{
    WindowMetaData(std::string windowStartFieldName, std::string windowEndFieldName)
        : windowStartFieldName(std::move(windowStartFieldName)), windowEndFieldName(std::move(windowEndFieldName))
    {
    }

    std::string windowStartFieldName;
    std::string windowEndFieldName;
};

/// Stores the information of a window. The start, end, and the identifier
struct WindowInfo
{
    WindowInfo(const Timestamp windowStart, const Timestamp windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    WindowInfo(const uint64_t windowStart, const uint64_t windowEnd) : windowStart(windowStart), windowEnd(windowEnd)
    {
        if (windowEnd < windowStart)
        {
            this->windowStart = Timestamp(0);
        }
    }

    bool operator<(const WindowInfo& other) const { return windowEnd < other.windowEnd; }
    Timestamp windowStart;
    Timestamp windowEnd;
};

/// Stores the metadata for a RecordBuffer
struct BufferMetaData
{
    BufferMetaData(const Timestamp watermarkTs, const SequenceData seqNumber, const OriginId originId)
        : watermarkTs(watermarkTs), seqNumber(seqNumber), originId(originId)
    {
    }

    [[nodiscard]] std::string toString() const
    {
        std::ostringstream oss;
        oss << "waterMarkTs: " << watermarkTs << ","
            << "seqNumber: " << seqNumber << ","
            << "originId: " << originId;
        return oss.str();
    }

    Timestamp watermarkTs;
    SequenceData seqNumber;
    OriginId originId;
};

namespace Util
{
Nautilus::VarVal createMinValue(const PhysicalTypePtr& physicalType);
Nautilus::VarVal createMaxValue(const PhysicalTypePtr& physicalType);
template <typename T>
static Nautilus::VarVal createConstValue(T value, const PhysicalTypePtr& physicalType)
{
    if (NES::Util::instanceOf<BasicPhysicalType>(physicalType))
    {
        const auto basicType = std::static_pointer_cast<BasicPhysicalType>(physicalType);
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


}

namespace NES::QueryCompilation::Util
{
/**
 * @brief Get the windowing parameter (size, slide, and time function) for the given window type
 * @param windowType
 * @return Tuple<WindowSize, WindowSlide, TimeFunction>
 */
std::tuple<uint64_t, uint64_t, Runtime::Execution::Operators::TimeFunctionPtr>
getWindowingParameters(Windowing::TimeBasedWindowType& windowType);

}
