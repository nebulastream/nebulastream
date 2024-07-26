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

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <nautilus/val.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void EventTimeFunction::open(Execution::ExecutionContext&, Execution::RecordBuffer&) {
    // nop
}

EventTimeFunction::EventTimeFunction(Expressions::ExpressionPtr timestampExpression, Windowing::TimeUnit unit)
    : unit(unit), timestampExpression(std::move(timestampExpression)) {}

UInt64 EventTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) {
    auto ts = this->timestampExpression->execute(record);
    const auto timeMultiplier = ExecutableDataType<uint64_t>::create(unit.getMillisecondsConversionMultiplier());
    auto tsInMs = (*ts * timeMultiplier);
    auto tsInMsVal = tsInMs->as<uint64_t>();
    ctx.setCurrentTs(tsInMsVal);
    return ExecutableDataType<uint64_t>::create(tsInMsVal);
}

void IngestionTimeFunction::open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) {
    ctx.setCurrentTs(buffer.getCreatingTs());
}

UInt64 IngestionTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record&) {
    return ExecutableDataType<uint64_t>::create(ctx.getCurrentTs());
}

}// namespace NES::Runtime::Execution::Operators
