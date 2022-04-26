
#include <Interpreter/Operators/ExecuteOperator.hpp>
#include <Interpreter/Operators/Scan.hpp>
#include <Interpreter/Record.hpp>
namespace NES::Interpreter {

void Scan::open(TraceContext& tracer)  {
    Value numberOfRecords = Value<>(20);
    auto v1 = Value<>(0);
    auto fields = std::vector<Value<>>({v1});
    for (auto i = Value<Integer>(0); i < numberOfRecords.as<Integer>(); i = i + 1) {
        Record record = Record(fields);
        child->execute(record);
    }
}

}// namespace NES::Interpreter