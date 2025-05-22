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

#include <memory>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <QueryCompiler/Phases/Translations/TimestampField.hpp>
#include <fmt/format.h>

const NES::QueryCompilation::TimestampField::TimeFunctionType& NES::QueryCompilation::TimestampField::getTimeFunctionType() const
{
    return timeFunctionType;
}

NES::Windowing::TimeUnit NES::QueryCompilation::TimestampField::getUnit() const
{
    return unit;
}

const std::string& NES::QueryCompilation::TimestampField::getName() const
{
    return fieldName;
}

std::unique_ptr<NES::Runtime::Execution::Operators::TimeFunction> NES::QueryCompilation::TimestampField::toTimeFunction() const
{
    switch (timeFunctionType)
    {
        case EVENT_TIME:
            return std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(
                std::make_unique<Runtime::Execution::Functions::ExecutableFunctionReadField>(fieldName), unit);
        case INGESTION_TIME:
            return std::make_unique<Runtime::Execution::Operators::IngestionTimeFunction>();
    }
}

NES::QueryCompilation::TimestampField NES::QueryCompilation::TimestampField::IngestionTime()
{
    return {"IngestionTime", Windowing::TimeUnit(1), INGESTION_TIME};
}

NES::QueryCompilation::TimestampField NES::QueryCompilation::TimestampField::EventTime(std::string fieldName, Windowing::TimeUnit tm)
{
    return {std::move(fieldName), std::move(tm), EVENT_TIME};
}

NES::QueryCompilation::TimestampField::TimestampField(
    std::string fieldName, Windowing::TimeUnit unit, const TimeFunctionType timeFunctionType)
    : fieldName(std::move(fieldName)), unit(std::move(unit)), timeFunctionType(timeFunctionType)
{
}

std::ostream& NES::QueryCompilation::operator<<(std::ostream& os, const TimestampField& obj)
{
    return os << fmt::format("TimestampField({}, {})", obj.fieldName, obj.unit.getMillisecondsConversionMultiplier());
}
