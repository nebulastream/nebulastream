#include <Interpreter/Record.hpp>

#include <Interpreter/Operations/AddOp.hpp>
#include <Interpreter/Operations/AndOp.hpp>
#include <Interpreter/Operations/DivOp.hpp>
#include <Interpreter/Operations/EqualsOp.hpp>
#include <Interpreter/Operations/LessThenOp.hpp>
#include <Interpreter/Operations/MulOp.hpp>
#include <Interpreter/Operations/NegateOp.hpp>
#include <Interpreter/Operations/OrOp.hpp>
#include <Interpreter/Operations/SubOp.hpp>

namespace NES::Interpreter {

Record::Record(std::vector<Value> records): records(std::move(records)) {}

Value& Record::read(uint64_t fieldIndex) {
    return records[fieldIndex];
}

void Record::write(uint64_t fieldIndex, Value& value) {
    records[fieldIndex] = value;
}

}