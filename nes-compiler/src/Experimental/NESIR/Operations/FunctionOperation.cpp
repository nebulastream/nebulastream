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

#include <Experimental/NESIR/Operations/FunctionOperation.hpp>
#include <utility>

namespace NES {

FunctionOperation::FunctionOperation(std::string name, BasicBlockPtr functionBasicBlock,
                                     std::vector<Operation::BasicType> inputArgs, std::vector<std::string> inputArgNames,
                                     Operation::BasicType outputArg)
    : Operation(OperationType::FunctionOp), name(std::move(name)), functionBasicBlock(std::move(functionBasicBlock)),
      inputArgs(std::move(inputArgs)),  inputArgNames(std::move(inputArgNames)), outputArg(outputArg){}

const std::string& FunctionOperation::getName() const { return name; }
BasicBlockPtr FunctionOperation::getFunctionBasicBlock() {
    return functionBasicBlock;
}
const std::vector<Operation::BasicType>& FunctionOperation::getInputArgs() const { return inputArgs; }
Operation::BasicType FunctionOperation::getOutputArg() const { return outputArg; }

bool FunctionOperation::classof(const NES::Operation* Op) { return Op->getOperationType() == OperationType::FunctionOp; }
const std::vector<std::string>& FunctionOperation::getInputArgNames() const { return inputArgNames; }

}// namespace NES
