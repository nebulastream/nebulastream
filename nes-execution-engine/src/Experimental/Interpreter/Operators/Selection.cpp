
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Experimental/Interpreter/Record.hpp>
namespace NES::Interpreter {

void Selection::execute(ExecutionContext& ctx, Record& record) const {
    if (expression->execute(record)) {
        child->execute(ctx, record);
    }
}

}// namespace NES::Interpreter