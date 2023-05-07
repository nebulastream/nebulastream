
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void EventTimeFunction::open(Execution::ExecutionContext&, Execution::RecordBuffer&) {
    // nop
}

EventTimeFunction::EventTimeFunction(Expressions::ExpressionPtr timestampExpression)
    : timestampExpression(std::move(timestampExpression)) {}

Nautilus::Value<UInt64> EventTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) {
    Value<UInt64> ts = this->timestampExpression->execute(record).as<UInt64>();
    ctx.setCurrentTs(ts);
    return ts;
}

void IngestionTimeFunction::open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) {
    ctx.setCurrentTs(buffer.getCreatingTs());
}

Nautilus::Value<UInt64> IngestionTimeFunction::getTs(Execution::ExecutionContext& ctx, Nautilus::Record&) {
    return ctx.getCurrentTs();
}

}// namespace NES::Runtime::Execution::Operators