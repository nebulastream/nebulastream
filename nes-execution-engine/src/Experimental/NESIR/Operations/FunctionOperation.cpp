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

namespace NES::ExecutionEngine::Experimental::IR::Operations {

FunctionOperation::FunctionOperation(std::string name, std::vector<Operation::BasicType> inputArgs, 
                                     std::vector<std::string> inputArgNames, Operation::BasicType outputArg)
    : Operation(OperationType::FunctionOp), name(std::move(name)), inputArgs(std::move(inputArgs)), 
      inputArgNames(std::move(inputArgNames)), outputArg(outputArg) {}

const std::string& FunctionOperation::getName() const { return name; }

BasicBlockPtr FunctionOperation::addFunctionBasicBlock(BasicBlockPtr functionBasicBlock) {
    this->functionBasicBlock = functionBasicBlock;
    return this->functionBasicBlock;
}

BasicBlockPtr FunctionOperation::getFunctionBasicBlock() {
    return functionBasicBlock;
}
const std::vector<Operation::BasicType>& FunctionOperation::getInputArgs() const { return inputArgs; }
Operation::BasicType FunctionOperation::getOutputArg() const { return outputArg; }


std::string FunctionOperation::toString() {
    std::string baseString = "FunctionOperation(";
    if(inputArgNames.size() > 0) {
        baseString += inputArgNames[0];
        for(int i = 1; i < (int) inputArgNames.size(); ++i) { 
            baseString += ", " + inputArgNames.at(i);
        }
    }
    return baseString + ")";
}
bool FunctionOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::FunctionOp; }
const std::vector<std::string>& FunctionOperation::getInputArgNames() const { return inputArgNames; }

}// namespace NES
