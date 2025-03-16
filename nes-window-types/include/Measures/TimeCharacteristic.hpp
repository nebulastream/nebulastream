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

#include <memory>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Measures/TimeUnit.hpp>

namespace NES
{

namespace Windowing
{

/// The time stamp characteristic represents if an window is in event or processing time.
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
    TimeCharacteristic(Type type, Schema::Field field, TimeUnit unit);

    static std::shared_ptr<TimeCharacteristic> createIngestionTime();

    /// @param unit the time unit of the EventTime, defaults to milliseconds
    /// @param field the field from which we want to extract the time.
    static std::shared_ptr<TimeCharacteristic> createEventTime(const std::shared_ptr<NodeFunction>& field, const TimeUnit& unit);
    static std::shared_ptr<TimeCharacteristic> createEventTime(const std::shared_ptr<NodeFunction>& field);
    Type getType() const;

    // Todo: implement with overloaded operator
    bool equals(const TimeCharacteristic& other) const;

    std::string toString() const;
    std::string getTypeAsString() const;
    TimeUnit getTimeUnit() const;

    void setTimeUnit(const TimeUnit& unit);

    Schema::Field field;

private:
    Type type;
    TimeUnit unit;
};
}
}
