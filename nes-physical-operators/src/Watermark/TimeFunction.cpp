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

#include <Watermark/TimeFunction.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <variant>
#include <DataTypes/StructData.hpp>
#include <DataTypes/TimeUnit.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Overloaded.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>

namespace NES
{

std::unique_ptr<TimeFunction> TimeFunction::create(const Windowing::BoundTimeCharacteristic& timeCharacteristic)
{
    return std::visit(
        Overloaded{
            [](const Windowing::IngestionTimeCharacteristic&) -> std::unique_ptr<TimeFunction>
            { return std::make_unique<IngestionTimeFunction>(); },
            [](const Windowing::BoundEventTimeCharacteristic& eventTime) -> std::unique_ptr<TimeFunction>
            {
                return std::make_unique<EventTimeFunction>(
                    FieldAccessPhysicalFunction{eventTime.field->getField().getLastName()}, eventTime.unit);
            }},
        timeCharacteristic);
}

void EventTimeFunction::open(ExecutionContext&, RecordBuffer&) const
{
    /// nop
}

EventTimeFunction::EventTimeFunction(PhysicalFunction timestampFunction, const Windowing::TimeUnit& unit)
    : unit(unit), timestampFunction(std::move(timestampFunction))
{
}

nautilus::val<Timestamp> EventTimeFunction::getTs(ExecutionContext& ctx, Record& record) const
{
    const VarVal timeField = this->timestampFunction.execute(record, ctx.pipelineMemoryProvider.arena);

    const auto timeMultiplier = nautilus::val<uint64_t>(unit.getMillisecondsConversionMultiplier());
    /// POC Timestamp DataType plugin: Differentiate between a StructData VarVal and a uint64 varval:
    /// Case StructData:
    ///     Get the underlying timestamp value, round to ms, return.
    /// Case UINT64:
    ///     Get value as uint64, multiply with timeMultiplier to get ms, return.
    const VarVal tsVarVal = timeField.customVisit(
        [&]<typename T>(const T& val) -> VarVal
        {
            if constexpr (std::is_same_v<T, StructData>)
            {
                const auto& structData = val;
                const VarVal tsInMicroSecondsVal = structData.at(0);
                const nautilus::val<uint64_t> tsMicroSeconds = tsInMicroSecondsVal.getRawValueAs<nautilus::val<uint64_t>>();
                /// Round to ms. Ideally, the whole system will soon use microseconds instead of milliseconds, making this obsolete.
                return tsMicroSeconds / nautilus::val<uint64_t>{1000};
            }
            else
            {
                const nautilus::val<uint64_t> ts = timeField.getRawValueAs<nautilus::val<uint64_t>>();
                return ts * timeMultiplier;
            }
        });
    const nautilus::val<Timestamp> tsInMs = nautilus::val<Timestamp>{tsVarVal.getRawValueAs<nautilus::val<uint64_t>>()};
    ctx.currentTs = tsInMs;
    return tsInMs;
}

void IngestionTimeFunction::open(ExecutionContext& ctx, RecordBuffer& buffer) const
{
    ctx.currentTs = buffer.getCreatingTs();
}

nautilus::val<Timestamp> IngestionTimeFunction::getTs(ExecutionContext& ctx, Record&) const
{
    return ctx.currentTs;
}

}
