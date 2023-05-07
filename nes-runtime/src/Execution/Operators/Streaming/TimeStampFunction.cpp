
#include <Execution/Operators/Streaming/TimeStampFunction.hpp>

namespace NES::Runtime::Execution::Operators {

void EventTimeStampFunction::open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) {}

Nautilus::Value<> EventTimeStampFunction::getTimeStamp(Nautilus::Record& record) {}


void IngestionTimeStampFunction::open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) {
   ctx.setCurrentTs(buffer.getCreationTs());
}

}