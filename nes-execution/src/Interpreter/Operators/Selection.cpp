
#include <Interpreter/Operators/Selection.hpp>
#include <Interpreter/Record.hpp>
namespace NES::Interpreter {

void Selection::execute(ExecutionContext& ctx, Record& record) const {
    if (expression->execute(record)) {
        child->execute(ctx, record);
    }
}

}// namespace NES::Interpreter