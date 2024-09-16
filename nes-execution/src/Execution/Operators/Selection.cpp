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
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Selection.cpp

#include <Execution/Operators/Selection.hpp>
========
#include <Execution/Functions/ExecutableFunctionWriteField.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/src/Execution/Functions/ExecutableFunctionWriteField.cpp
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators
{
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Selection.cpp

void Selection::execute(ExecutionContext& ctx, Record& record) const
========
ExecutableFunctionWriteField::ExecutableFunctionWriteField(Record::RecordFieldIdentifier field, FunctionPtr subFunction)
    : field(std::move(field)), subFunction(std::move(subFunction))
{
}

VarVal ExecutableFunctionWriteField::execute(Record& record) const
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/src/Execution/Functions/ExecutableFunctionWriteField.cpp
{
    /// evaluate function and call child operator if function is valid
    if (function->execute(record))
    {
        child->execute(ctx, record);
    }
}

}
