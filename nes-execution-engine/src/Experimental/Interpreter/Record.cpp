/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
#include <Experimental/Interpreter/Exceptions/InterpreterException.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

Record::Record(std::vector<Value<Any>> records): records(std::move(records)) {}

Value<Any>& Record::read(uint64_t fieldIndex) {
    if(fieldIndex >= records.size()){
        throw new InterpreterException("Filed dose not exists");
    }
    return records[fieldIndex];
}

void Record::write(uint64_t fieldIndex, Value<Any>& value) {
    records[fieldIndex] = value;
}

}