
#include <Interpreter/Operators/ExecuteOperator.hpp>
#include <Interpreter/Operators/Scan.hpp>
#include <Interpreter/Record.hpp>
namespace NES::Interpreter {

void Scan::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    child->open(ctx, recordBuffer);
    auto numberOfRecords = recordBuffer.getNumRecords();
    for (Value<Integer> i = 0; i < numberOfRecords; i = i + 1) {
        auto record = recordBuffer.read(i);
        child->execute(ctx,record);
    }
}

}// namespace NES::Interpreter