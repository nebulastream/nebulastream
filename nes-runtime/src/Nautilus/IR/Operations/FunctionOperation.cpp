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

#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <utility>

namespace NES::Nautilus::IR::Operations {

FunctionOperation::FunctionOperation(std::string name,
                                     std::vector<PrimitiveStamp> inputArgs,
                                     std::vector<std::string> inputArgNames,
                                     Types::StampPtr outputArg)
    : Operation(OperationType::FunctionOp, outputArg), name(std::move(name)), inputArgs(std::move(inputArgs)),
      inputArgNames(std::move(inputArgNames)) {}

const std::string& FunctionOperation::getName() const { return name; }

BasicBlockPtr FunctionOperation::addFunctionBasicBlock(BasicBlockPtr functionBasicBlock) {
    this->functionBasicBlock = functionBasicBlock;
    return this->functionBasicBlock;
}

BasicBlockPtr FunctionOperation::getFunctionBasicBlock() { return functionBasicBlock; }
const std::vector<PrimitiveStamp>& FunctionOperation::getInputArgs() const { return inputArgs; }
Types::StampPtr FunctionOperation::getOutputArg() const { return getStamp(); }

std::string FunctionOperation::toString() {
    std::string baseString = name + '(';
    if (inputArgNames.size() > 0) {
        baseString += inputArgNames[0];
        for (int i = 1; i < (int) inputArgNames.size(); ++i) {
            baseString += ", " + inputArgNames.at(i);
        }
    }
    return baseString + ')';
}
bool FunctionOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::FunctionOp; }
const std::vector<std::string>& FunctionOperation::getInputArgNames() const { return inputArgNames; }

}// namespace NES::Nautilus::IR::Operations
