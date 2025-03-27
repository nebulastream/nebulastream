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
#include <utility>
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Measures/TimeUnit.hpp>

namespace NES::QueryCompilation
{
/**
 * A TimestampField is a wrapper around a FieldName and a Unit of time.
 * This enforces fields carrying time values to be evaluated with respect to a specific timeunit.
 */
class TimestampField
{
    enum TimeFunctionType
    {
        EVENT_TIME,
        INGESTION_TIME,
    };

public:
    friend std::ostream& operator<<(std::ostream& os, const TimestampField& obj);
    /**
     * The multiplier is the value which converts from the underlying time value to milliseconds.
     * E.g. the multiplier for a timestampfield of seconds is 1000
     * @return Unit Multiplier
     */
    [[nodiscard]] Windowing::TimeUnit getUnit() const;

    /**
     * Name of the field
     * @return reference to the field name
     */
    [[nodiscard]] const IdentifierList& getName() const;

    [[nodiscard]] const TimeFunctionType& getTimeFunctionType() const;

    /**
     * Builds the TimeFunction
     * @return reference to the field name
     */
    [[nodiscard]] std::unique_ptr<Runtime::Execution::Operators::TimeFunction> toTimeFunction() const;
    static TimestampField IngestionTime();
    static TimestampField EventTime(IdentifierList fieldName, Windowing::TimeUnit tm);

private:
    IdentifierList fieldName;
    Windowing::TimeUnit unit;
    TimeFunctionType timeFunctionType;
    TimestampField(IdentifierList fieldName, Windowing::TimeUnit unit, TimeFunctionType timeFunctionType);
};
}
