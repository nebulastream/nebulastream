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

#include <cstdint>
#include <utility>
#include <API/TimeUnit.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>

namespace NES
{

void EventTimeFunction::open(ExecutionContext&, RecordBuffer&) const
{
    /// nop
}

EventTimeFunction::EventTimeFunction(PhysicalFunction timestampFunction, const Windowing::TimeUnit& unit)
    : unit(unit), timestampFunction(std::move(std::move(timestampFunction)))
{
}

nautilus::val<Timestamp> EventTimeFunction::getTs(ExecutionContext& ctx, Record& record) const
{
    const auto ts = this->timestampFunction.execute(record, ctx.pipelineMemoryProvider.arena).cast<nautilus::val<uint64_t>>();
    const auto timeMultiplier = nautilus::val<uint64_t>(unit.getMillisecondsConversionMultiplier());
    const auto tsInMs = nautilus::val<Timestamp>(ts * timeMultiplier);
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
