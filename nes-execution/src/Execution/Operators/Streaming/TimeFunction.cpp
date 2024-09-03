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
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Runtime::Execution::Operators
{

void EventTimeFunction::open(Execution::ExecutionContext&, Execution::RecordBuffer&)
{
    /// nop
}

EventTimeFunction::EventTimeFunction(Expressions::ExpressionPtr timestampExpression, Windowing::TimeUnit unit)
    : unit(unit), timestampExpression(std::move(timestampExpression))
{
}

nautilus::val<uint64_t> EventTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record)
{
    const auto ts = this->timestampExpression->execute(record);
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

} /// namespace NES::Runtime::Execution::Operators
