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
<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Map.cpp
========
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp

#include <Execution/Operators/Map.hpp>
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Operators
{

<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Map.cpp
void Map::execute(ExecutionContext& ctx, Record& record) const
========
ExecutableFunctionReadField::ExecutableFunctionReadField(Record::RecordFieldIdentifier field) : field(field)
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp
{
    /// assume that map function performs a field write
    mapFunction->execute(record);
    /// call next operator
    child->execute(ctx, record);
}

<<<<<<<< HEAD:nes-execution/src/Execution/Operators/Map.cpp
========
VarVal ExecutableFunctionReadField::execute(Record& record) const
{
    return record.read(field);
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp
}
