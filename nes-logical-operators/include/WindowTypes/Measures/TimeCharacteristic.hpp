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

#include <API/AttributeField.hpp>
#include <API/TimeUnit.hpp>
#include <Functions/LogicalFunction.hpp>

namespace NES::Windowing
{

/// @brief The time stamp characteristic represents if a window is in event or processing time.
class TimeCharacteristic final
{
public:
    constexpr static auto RECORD_CREATION_TS_FIELD_NAME = "$record.creationTs";

    enum class Type : uint8_t
    {
        IngestionTime,
        EventTime
    };
    explicit TimeCharacteristic(Type type);
    TimeCharacteristic(Type type, AttributeField field, TimeUnit unit);

    /// @brief Factory to create a time characteristic for ingestion time window
    /// @param unit the time unit of the ingestion time
    /// @return std::shared_ptr<TimeCharacteristic>
    static TimeCharacteristic createIngestionTime();

    /// @brief Factory to create a event time window with an time extractor on a specific field.
    /// @param unit the time unit of the EventTime, defaults to milliseconds
    /// @param field the field from which we want to extract the time.
    /// @return std::shared_ptr<TimeCharacteristic>
    static TimeCharacteristic createEventTime(LogicalFunction field, const TimeUnit& unit);
    static TimeCharacteristic createEventTime(LogicalFunction field);

    /// @return The TimeCharacteristic type.
    [[nodiscard]] Type getType() const;

    [[nodiscard]] bool operator==(const TimeCharacteristic& other) const;

    [[nodiscard]] uint64_t hash() const;

    [[nodiscard]] std::string toString() const;
    [[nodiscard]] std::string getTypeAsString() const;
    [[nodiscard]] TimeUnit getTimeUnit() const;

    void setTimeUnit(const TimeUnit& unit);

    AttributeField field;

private:
    Type type;
    TimeUnit unit;
};
}
