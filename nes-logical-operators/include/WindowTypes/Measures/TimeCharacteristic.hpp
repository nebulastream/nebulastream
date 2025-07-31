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
#include <ostream>
#include <string>

#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Util/Logger/Formatter.hpp>
#include "Functions/UnboundFieldAccessLogicalFunction.hpp"
#include "Identifiers/Identifier.hpp"


namespace NES::Windowing
{
struct IngestionTimeCharacteristic
{
    [[nodiscard]] constexpr bool operator==(const IngestionTimeCharacteristic&) { return true; }

    friend std::ostream& operator<<(std::ostream& os, const IngestionTimeCharacteristic& timeCharacteristic);
};

struct UnboundEventTimeCharacteristic
{
    UnboundFieldAccessLogicalFunction field;
    TimeUnit unit{1};
    [[nodiscard]] bool operator==(const UnboundEventTimeCharacteristic& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const IngestionTimeCharacteristic& timeCharacteristic);
};

struct BoundEventTimeCharacteristic
{
    FieldAccessLogicalFunction field;
    TimeUnit unit{1};
    [[nodiscard]] bool operator==(const BoundEventTimeCharacteristic& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const IngestionTimeCharacteristic& timeCharacteristic);
};

using UnboundTimeCharacteristic = std::variant<IngestionTimeCharacteristic, UnboundEventTimeCharacteristic>;
using BoundTimeCharacteristic = std::variant<IngestionTimeCharacteristic, BoundEventTimeCharacteristic>;
using TimeCharacteristic = std::variant<UnboundTimeCharacteristic, BoundTimeCharacteristic>;

/// @brief The timestamp characteristic implements time information for windowed operators
class TimeCharacteristicWrapper final
{
public:
    enum class Type : uint8_t
    {
        IngestionTime,
        EventTime
    };

    explicit TimeCharacteristicWrapper(std::variant<UnboundTimeCharacteristic, BoundTimeCharacteristic> timeCharacteristic);
    static IngestionTimeCharacteristic createIngestionTime();
    static UnboundEventTimeCharacteristic createEventTime(UnboundFieldAccessLogicalFunction field, const TimeUnit& unit = TimeUnit{1});
    static BoundEventTimeCharacteristic createEventTime(FieldAccessLogicalFunction field, const TimeUnit& unit = TimeUnit{1});

    /// @return The TimeCharacteristic type.
    [[nodiscard]] Type getType() const;

    [[nodiscard]] bool operator==(const TimeCharacteristicWrapper& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const TimeCharacteristicWrapper& timeCharacteristic);

    [[nodiscard]] std::string getTypeAsString() const;
    [[nodiscard]] TimeCharacteristic getUnderlying() const;
    [[nodiscard]] TimeCharacteristic&& getUnderlying() &&;
    operator const TimeCharacteristic&() const;

    [[nodiscard]] BoundTimeCharacteristic withInferredSchema(const Schema& schema) const;

private:
    TimeCharacteristic underlying;
};

}

FMT_OSTREAM(NES::Windowing::TimeCharacteristic);

template <>
struct std::hash<NES::Windowing::IngestionTimeCharacteristic>
{
    std::size_t operator()(const NES::Windowing::IngestionTimeCharacteristic& timeCharacteristic) const noexcept;
};

template <>
struct std::hash<NES::Windowing::UnboundEventTimeCharacteristic>
{
    std::size_t operator()(const NES::Windowing::UnboundEventTimeCharacteristic& timeCharacteristic) const noexcept;
};

template <>
struct std::hash<NES::Windowing::BoundEventTimeCharacteristic>
{
    std::size_t operator()(const NES::Windowing::BoundEventTimeCharacteristic& timeCharacteristic) const noexcept;
};

