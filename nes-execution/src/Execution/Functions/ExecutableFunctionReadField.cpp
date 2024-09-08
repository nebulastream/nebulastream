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
<<<<<<<< HEAD:nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
========
#include <Execution/Functions/ReadFieldFunction.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-execution/src/Execution/Functions/ReadFieldFunction.cpp

namespace NES::Runtime::Execution::Functions
{

<<<<<<<< HEAD:nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp
ExecutableFunctionReadField::ExecutableFunctionReadField(Record::RecordFieldIdentifier field) : field(field)
{
}

VarVal ExecutableFunctionReadField::execute(Record& record) const
========
ReadFieldFunction::ReadFieldFunction(Record::RecordFieldIdentifier field) : field(field)
{
}

VarVal ReadFieldFunction::execute(Record& record) const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-execution/src/Execution/Functions/ReadFieldFunction.cpp
{
    return record.read(field);
}

<<<<<<<< HEAD:nes-execution/src/Execution/Functions/ExecutableFunctionReadField.cpp
}
========
} /// namespace NES::Runtime::Execution::Functions
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-execution/src/Execution/Functions/ReadFieldFunction.cpp
