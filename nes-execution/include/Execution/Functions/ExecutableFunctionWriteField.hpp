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

#pragma once

#include <Execution/Functions/Function.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Functions
{

/**
 * @brief This function writes a particular Value to a specific field of an record.
 */
class ExecutableFunctionWriteField : public Function
{
public:
    ExecutableFunctionWriteField(Record::RecordFieldIdentifier field, std::unique_ptr<Function> childFunction);
    VarVal execute(Record& record) const override;

private:
    const Record::RecordFieldIdentifier field;
    const std::unique_ptr<Function> childFunction;
};
}
