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

#include <utility>
#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES::Runtime::Execution::Operators
{

void EventTimeFunction::open(ExecutionContext&, RecordBuffer&)
{
    /// nop
}

EventTimeFunction::EventTimeFunction(std::unique_ptr<Functions::Function> timestampFunction, Windowing::TimeUnit unit)
    : unit(unit), timestampFunction(std::move(timestampFunction))
{
}

nautilus::val<Timestamp> EventTimeFunction::getTs(ExecutionContext& ctx, Record& record)
{
    const auto ts = this->timestampFunction->execute(record).cast<nautilus::val<uint64_t>>();
    const auto timeMultiplier = nautilus::val<uint64_t>(unit.getMillisecondsConversionMultiplier());
    const auto tsInMs = nautilus::val<Timestamp>(ts * timeMultiplier);
    ctx.currentTs = tsInMs;
    return tsInMs;
}

void IngestionTimeFunction::open(ExecutionContext& ctx, RecordBuffer& buffer)
{
    ctx.currentTs = buffer.getCreatingTs();
}

nautilus::val<Timestamp> IngestionTimeFunction::getTs(ExecutionContext& ctx, Record&)
{
    return ctx.currentTs;
}

}
