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
#include <tuple>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class TimeFunction;
class EventTimeFunction;
class IngestionTimeFunction;

/// A TimestampField is a wrapper around a FieldName, a Unit of time and a time function type.
/// This enforces fields carrying time values to be evaluated with respect to a specific timeunit.
class TimestampField
{
    enum TimeFunctionType : uint8_t
    {
        EVENT_TIME,
        INGESTION_TIME,
    };

public:
    friend std::ostream& operator<<(std::ostream& os, const TimestampField& obj);

    /// Builds the TimeFunction
    [[nodiscard]] std::unique_ptr<TimeFunction> toTimeFunction() const
    {
        switch (timeFunctionType)
        {
            case EVENT_TIME:
                return std::make_unique<EventTimeFunction>(FieldAccessPhysicalFunction(fieldName), unit);
            case INGESTION_TIME:
                return std::make_unique<IngestionTimeFunction>();
        }
    }

    static TimestampField ingestionTime() { return {"IngestionTime", Windowing::TimeUnit(1), INGESTION_TIME}; }

    static TimestampField eventTime(std::string fieldName, const Windowing::TimeUnit& timeUnit)
    {
        return {std::move(fieldName), timeUnit, EVENT_TIME};
    }

    static std::tuple<TimestampField, TimestampField>
    getTimestampLeftAndRight(const JoinLogicalOperator& joinOperator, const std::shared_ptr<Windowing::TimeBasedWindowType>& windowType)
    {
        if (windowType->getTimeCharacteristic().getType() == Windowing::TimeCharacteristic::Type::IngestionTime)
        {
            NES_DEBUG("Skip eventime identification as we use ingestion time");
            return {TimestampField::ingestionTime(), TimestampField::ingestionTime()};
        }

        const auto timeStampFieldNameWithoutSourceName = windowType->getTimeCharacteristic().field.getUnqualifiedName();

        /// Extracting the left and right timestamp
        const auto timeStampFieldNameLeft = joinOperator.getInputSchemas().at(0).getFieldByName(timeStampFieldNameWithoutSourceName);
        const auto timeStampFieldNameRight = joinOperator.getInputSchemas().at(1).getFieldByName(timeStampFieldNameWithoutSourceName);

        INVARIANT(
            timeStampFieldNameLeft.has_value() and timeStampFieldNameRight.has_value(),
            "Could not find timestampfieldname {} in both streams!",
            timeStampFieldNameWithoutSourceName);

        return {
            TimestampField::eventTime(timeStampFieldNameLeft.value().name, windowType->getTimeCharacteristic().getTimeUnit()),
            TimestampField::eventTime(timeStampFieldNameRight.value().name, windowType->getTimeCharacteristic().getTimeUnit())};
    }

private:
    std::string fieldName;
    Windowing::TimeUnit unit;
    TimeFunctionType timeFunctionType;

    TimestampField(std::string fieldName, const Windowing::TimeUnit& unit, TimeFunctionType timeFunctionType)
        : fieldName(std::move(fieldName)), unit(unit), timeFunctionType(timeFunctionType)
    {
    }
};


}
