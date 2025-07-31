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

#include <Serialization/TimeCharacteristicSerializationUtil.hpp>
#include "Serialization/FunctionSerializationUtil.hpp"

namespace NES
{
SerializableTimeCharacteristic* TimeCharacteristicSerializationUtil::serializeCharacteristic(
    const Windowing::TimeCharacteristic& characteristic, SerializableTimeCharacteristic* serializedCharacteristic)
{
    std::visit(
        [&serializedCharacteristic](const auto& boundOrUnbound)
        {
            std::visit(
                [&](const auto& ingestionOrEvent)
                {
                    using CharacteristicType = std::decay_t<decltype(ingestionOrEvent)>;
                    if constexpr (std::is_same_v<CharacteristicType, Windowing::IngestionTimeCharacteristic>)
                    {
                    }
                    else if constexpr (
                        std::is_same_v<CharacteristicType, Windowing::UnboundEventTimeCharacteristic>
                        || std::is_same_v<CharacteristicType, Windowing::BoundEventTimeCharacteristic>)
                    {
                        auto* serializedEventTimeCharacteristic = serializedCharacteristic->mutable_eventtime();
                        serializedEventTimeCharacteristic->set_multiplier(
                            ingestionOrEvent.unit.getMillisecondsConversionMultiplier());
                        serializedEventTimeCharacteristic->mutable_field()->CopyFrom(ingestionOrEvent.field.serialize());
                    }
                    else
                    {
                        static_assert(false, "Lacking serialization for time characteristic");
                    }
                },
                boundOrUnbound);
        },
        characteristic);
    return serializedCharacteristic;
}

std::optional<Windowing::UnboundTimeCharacteristic>
TimeCharacteristicSerializationUtil::deserializeUnboundCharacteristic(const SerializableTimeCharacteristic& serializedCharacteristic)
{
    if (!serializedCharacteristic.has_eventtime())
    {
        return Windowing::IngestionTimeCharacteristic{};
    }
    const auto& serializedEventTimeCharacteristic = serializedCharacteristic.eventtime();
    const auto unit = Windowing::TimeUnit{serializedEventTimeCharacteristic.multiplier()};
    if (serializedEventTimeCharacteristic.field().function_type() != UnboundFieldAccessLogicalFunction::NAME)
    {
        return std::nullopt;
    }
    const auto function = FunctionSerializationUtil::deserializeFunction(serializedEventTimeCharacteristic.field(), Schema{});
    const auto fieldAccessOpt = function.tryGet<UnboundFieldAccessLogicalFunction>();
    INVARIANT(
        fieldAccessOpt != std::nullopt,
        "Function in event time characteristic had name of {} but was not deserialized to it",
        UnboundFieldAccessLogicalFunction::NAME);
    return Windowing::UnboundEventTimeCharacteristic{.field = fieldAccessOpt.value(), .unit = unit};
}

std::optional<Windowing::BoundTimeCharacteristic> TimeCharacteristicSerializationUtil::deserializeBoundCharacteristic(
    const SerializableTimeCharacteristic& serializedCharacteristic, const Schema& schema)
{
    if (!serializedCharacteristic.has_eventtime())
    {
        return Windowing::IngestionTimeCharacteristic{};
    }
    const auto& serializedEventTimeCharacteristic = serializedCharacteristic.eventtime();
    const auto unit = Windowing::TimeUnit{serializedEventTimeCharacteristic.multiplier()};
    if (serializedEventTimeCharacteristic.field().function_type() != FieldAccessLogicalFunction::NAME)
    {
        return std::nullopt;
    }
    const auto function = FunctionSerializationUtil::deserializeFunction(serializedEventTimeCharacteristic.field(), schema);
    const auto fieldAccessOpt = function.tryGet<FieldAccessLogicalFunction>();
    INVARIANT(
        fieldAccessOpt != std::nullopt,
        "Function in event time characteristic had name of {} but was not deserialized to it",
        FieldAccessLogicalFunction::NAME);
    return Windowing::BoundEventTimeCharacteristic{.field = fieldAccessOpt.value(), .unit = unit};
}
}
