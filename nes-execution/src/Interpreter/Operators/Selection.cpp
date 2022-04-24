
#include <Interpreter/Operators/Selection.hpp>
#include <Interpreter/Record.hpp>
namespace NES::Interpreter {

void Selection::execute(Record& record) {
    if (expression->execute(record)) {
        child->execute(record);
    }
}

void Selection::open(TraceContext&) {}
}// namespace NES::Interpreter