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
#include <Experimental/Interpreter/DataValue/Any.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/WriteFieldExpression.hpp>
#include <Experimental/Interpreter/Record.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

Value<> WriteFieldExpression::execute(Record& record) {
    Value<> newValue = subExpression->execute(record);
    record.write(fieldIndex, newValue);
    return newValue;
}
WriteFieldExpression::WriteFieldExpression(uint64_t fieldIndex, const ExpressionPtr& subExpression)
    : fieldIndex(fieldIndex), subExpression(subExpression) {}
}// namespace NES::ExecutionEngine::Experimental::Interpreter