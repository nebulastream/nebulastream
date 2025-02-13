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
#include <Execution/Functions/ExecutableFunctionReadField.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES::Runtime::Execution::Functions
{

ExecutableFunctionReadField::ExecutableFunctionReadField(Record::RecordFieldIdentifier field) : field(field)
{
}

VarVal ExecutableFunctionReadField::execute(const Record& record, ArenaRef&) const
{
    return record.read(field);
}

}
