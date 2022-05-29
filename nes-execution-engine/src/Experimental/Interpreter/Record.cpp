#include <Experimental/Interpreter/Record.hpp>

#include <Experimental/Interpreter/Operations/AddOp.hpp>
#include <Experimental/Interpreter/Operations/AndOp.hpp>
#include <Experimental/Interpreter/Operations/DivOp.hpp>
#include <Experimental/Interpreter/Operations/EqualsOp.hpp>
#include <Experimental/Interpreter/Operations/LessThenOp.hpp>
#include <Experimental/Interpreter/Operations/MulOp.hpp>
#include <Experimental/Interpreter/Operations/NegateOp.hpp>
#include <Experimental/Interpreter/Operations/OrOp.hpp>
#include <Experimental/Interpreter/Operations/SubOp.hpp>

namespace NES::Interpreter {

Record::Record(std::vector<Value<Any>> records): records(std::move(records)) {}

Value<Any>& Record::read(uint64_t fieldIndex) {
    return records[fieldIndex];
}

void Record::write(uint64_t fieldIndex, Value<Any>& value) {
    records[fieldIndex] = value;
}

}