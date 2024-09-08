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

<<<<<<<< HEAD:nes-functions/include/Functions/ArithmeticalFunctions/NodeFunctionArithmetical.hpp
namespace NES
{
/**
 * @brief This class just indicates that a node is an arithmetical function.
 */
class NodeFunctionArithmetical
{
protected:
    NodeFunctionArithmetical() = default;
    virtual ~NodeFunctionArithmetical() noexcept = default;
};

}
========
#include <Execution/Functions/Function.hpp>

namespace NES::Runtime::Execution::Functions {

/// Performs leftSubFunction + rightSubFunction
class AddFunction : public Function {
  public:
    AddFunction(FunctionPtr leftSubFunction, FunctionPtr rightSubFunction);
    VarVal execute(Record& record) const override;

  private:
    const FunctionPtr leftSubFunction;
    const FunctionPtr rightSubFunction;
};

}
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-execution/include/Execution/Functions/ArithmeticalFunctions/AddFunction.hpp
