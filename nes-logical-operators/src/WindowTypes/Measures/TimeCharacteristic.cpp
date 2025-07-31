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

#include "WindowTypes/Measures/TimeCharacteristic.hpp"


#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <folly/Hash.h>

#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Schema/Field.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>

namespace NES::Windowing
{


std::ostream& operator<<(std::ostream& os, const IngestionTimeCharacteristic&)
{
    return os << fmt::format("IngestionTimeCharacteristic()");
}

std::ostream& operator<<(std::ostream& os, const UnboundEventTimeCharacteristic& timeCharacteristic)
{
    return os << fmt::format(
               "UnboundEventTimeCharacteristic(field: {}, unit: {})", timeCharacteristic.field.getFieldName(), timeCharacteristic.unit);
}

std::ostream& operator<<(std::ostream& os, const BoundEventTimeCharacteristic& timeCharacteristic)
{
    return os << fmt::format(
               "BoundEventTimeCharacteristic(field: {}, unit: {})",
               timeCharacteristic.field.getField().getLastName(),
               timeCharacteristic.unit);
}

IngestionTimeCharacteristic TimeCharacteristicWrapper::createIngestionTime()
{
    return IngestionTimeCharacteristic{};
}

UnboundEventTimeCharacteristic TimeCharacteristicWrapper::createEventTime(UnboundFieldAccessLogicalFunction field, const TimeUnit& unit)
{
    return UnboundEventTimeCharacteristic{.field = std::move(field), .unit = unit};
}

BoundEventTimeCharacteristic TimeCharacteristicWrapper::createEventTime(FieldAccessLogicalFunction field, const TimeUnit& unit)
{
    return BoundEventTimeCharacteristic{.field = std::move(field), .unit = unit};
}

TimeCharacteristicWrapper::TimeCharacteristicWrapper(std::variant<UnboundTimeCharacteristic, BoundTimeCharacteristic> timeCharacteristic)
    : underlying{std::move(timeCharacteristic)}
{
}

std::string TimeCharacteristicWrapper::getTypeAsString() const
{
    switch (getType())
    {
        case Type::IngestionTime:
            return "IngestionTime";
        case Type::EventTime:
            return "EventTime";
        default:
            return "Unknown TimeCharacteristic type";
    }
}

TimeCharacteristicWrapper::Type TimeCharacteristicWrapper::getType() const
{
    return std::visit(
        [](const auto& timeCharacteristicVar)
        {
            return std::visit(
                [](const auto& timeCharacteristic)
                {
                    using CharacteristicType = std::decay_t<decltype(timeCharacteristic)>;
                    if constexpr (std::is_same_v<IngestionTimeCharacteristic, CharacteristicType>)
                    {
                        return Type::IngestionTime;
                    }
                    else if constexpr (
                        std::is_same_v<UnboundEventTimeCharacteristic, CharacteristicType>
                        || std::is_same_v<BoundEventTimeCharacteristic, CharacteristicType>)
                    {
                        return Type::EventTime;
                    }
                    else
                    {
                        static_assert(false, "Missing conversion of time characteristic type to string");
                    }
                },
                timeCharacteristicVar);
        },
        underlying);
}

BoundTimeCharacteristic TimeCharacteristicWrapper::withInferredSchema(const Schema& schema) const
{
    return std::visit(
        [&schema](const auto& unboundBound)
        {
            return std::visit(
                Overloaded{
                    [](const IngestionTimeCharacteristic& ingestionTimeCharacteristic)
                    { return BoundTimeCharacteristic{ingestionTimeCharacteristic}; },
                    [&](const UnboundEventTimeCharacteristic& unboundEventTimeCharacteristic)
                    {
                        const auto erasedFieldAccess = unboundEventTimeCharacteristic.field.withInferredDataType(schema);
                        const auto fieldAccessOpt = erasedFieldAccess.tryGet<FieldAccessLogicalFunction>();

                        return BoundTimeCharacteristic{
                            BoundEventTimeCharacteristic{.field = fieldAccessOpt.value(), .unit = unboundEventTimeCharacteristic.unit}};
                    },
                    [&](const BoundEventTimeCharacteristic& boundEventTimeCharacteristic)
                    {
                        const auto erasedFieldAccess = boundEventTimeCharacteristic.field.withInferredDataType(schema);
                        const auto fieldAccessOpt = erasedFieldAccess.tryGet<FieldAccessLogicalFunction>();

                        return BoundTimeCharacteristic{
                            BoundEventTimeCharacteristic{.field = fieldAccessOpt.value(), .unit = boundEventTimeCharacteristic.unit}};
                    },
                },
                unboundBound);
        },
        underlying);
}

TimeCharacteristic TimeCharacteristicWrapper::getUnderlying() const
{
    return underlying;
}

TimeCharacteristic&& TimeCharacteristicWrapper::getUnderlying() &&
{
    return std::move(underlying);
}

TimeCharacteristicWrapper::operator const std::variant<UnboundTimeCharacteristic, BoundTimeCharacteristic>&() const
{
    return underlying;
}

std::ostream& operator<<(std::ostream& os, const TimeCharacteristicWrapper& timeCharacteristic)
{
    std::visit(
        [&](const auto& boundOrUnbound)
        { return std::visit([&](const auto& ingestionOrEvent) { os << ingestionOrEvent; }, boundOrUnbound); },
        timeCharacteristic.underlying);
    return os;
}
}

std::size_t std::hash<NES::Windowing::IngestionTimeCharacteristic>::operator()(
    const NES::Windowing::IngestionTimeCharacteristic&) const noexcept
{
    return 983131;
}

std::size_t std::hash<NES::Windowing::UnboundEventTimeCharacteristic>::operator()(
    const NES::Windowing::UnboundEventTimeCharacteristic& timeCharacteristic) const noexcept
{
    return folly::hash::hash_combine(timeCharacteristic.field, timeCharacteristic.unit);
}

std::size_t std::hash<NES::Windowing::BoundEventTimeCharacteristic>::operator()(
    const NES::Windowing::BoundEventTimeCharacteristic& timeCharacteristic) const noexcept
{
    return folly::hash::hash_combine(timeCharacteristic.field, timeCharacteristic.unit);
}
