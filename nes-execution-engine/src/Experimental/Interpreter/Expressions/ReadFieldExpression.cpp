#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>

namespace NES::Interpreter {

ReadFieldExpression::ReadFieldExpression(uint64_t fieldIndex) : fieldIndex(fieldIndex) {}

Value<> ReadFieldExpression::execute(Record& record) { return record.read(fieldIndex); }

}// namespace NES::Interpreter