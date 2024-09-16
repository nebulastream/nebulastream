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

namespace NES::Runtime::Execution::Functions
{

<<<<<<<< HEAD:nes-execution/include/Execution/Functions/ArithmeticalFunctions/ExecutableFunctionAdd.hpp
/// Performs leftExecutableFunctionSub + rightExecutableFunctionSub
class ExecutableFunctionAdd : public Function
{
public:
    ExecutableFunctionAdd(std::unique_ptr<Function> leftExecutableFunctionSub, std::unique_ptr<Function> rightExecutableFunctionSub);
========
/**
 * @brief This function writes a particular Value to a specific field of an record.
 */
class ExecutableFunctionWriteField : public Function
{
public:
    ExecutableFunctionWriteField(Record::RecordFieldIdentifier field, FunctionPtr subFunction);
>>>>>>>> 5405d3812e (chore(Functions) Renamed Function --> ExecutableFunction):nes-execution/include/Execution/Functions/ExecutableFunctionWriteField.hpp
    VarVal execute(Record& record) const override;

private:
    const std::unique_ptr<Function> leftExecutableFunctionSub;
    const std::unique_ptr<Function> rightExecutableFunctionSub;
};

}
