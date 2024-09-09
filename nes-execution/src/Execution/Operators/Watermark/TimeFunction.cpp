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
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Watermark/TimeFunction.cpp
#include <Execution/Operators/Watermark/TimeFunction.hpp>
========
#include <Execution/Operators/Streaming/Watermark/TimeFunction.hpp>
>>>>>>>> 68726787a8 (chore(Execution) Moved Watermark operators to own folder):nes-execution/src/Execution/Operators/Streaming/Watermark/TimeFunction.cpp
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/NESStrongTypeRef.hpp>

namespace NES::Runtime::Execution::Operators
{

void EventTimeFunction::open(Execution::ExecutionContext&, Execution::RecordBuffer&)
{
    /// nop
}

EventTimeFunction::EventTimeFunction(std::unique_ptr<Functions::Function> timestampFunction, Windowing::TimeUnit unit)
    : unit(unit), timestampFunction(std::move(timestampFunction))
{
}

nautilus::val<WatermarkTs> EventTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record)
{
    const auto ts = this->timestampFunction->execute(record);
    const auto timeMultiplier = nautilus::val<uint64_t>(unit.getMillisecondsConversionMultiplier());
    const auto tsInMs = (ts * timeMultiplier).cast<nautilus::val<uint64_t>>();
    ctx.setCurrentTs(tsInMs);
    return tsInMs;
}

void IngestionTimeFunction::open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer)
{
    ctx.setCurrentTs(buffer.getCreatingTs());
}

nautilus::val<uint64_t> IngestionTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record&)
{
    return ctx.getCurrentTs();
}

}
