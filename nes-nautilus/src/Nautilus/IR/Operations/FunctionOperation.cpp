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

#include <Nautilus/IR/Types/BasicTypes.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/FunctionOperation.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <string>
#include <utility>
#include <vector>

namespace NES::Nautilus::IR::Operations {

FunctionOperation::FunctionOperation(std::string name,
                                     std::vector<PrimitiveStamp> inputArgs,
                                     std::vector<std::string> inputArgNames,
                                     Types::StampPtr outputArg)
    : Operation(OperationType::FunctionOp, std::move(outputArg)), name(std::move(name)), inputArgs(std::move(inputArgs)),
      inputArgNames(std::move(inputArgNames)) {}

const std::string& FunctionOperation::getName() const { return name; }

const BasicBlock* FunctionOperation::addFunctionBasicBlock(BasicBlock& functionBasicBlock) {
    this->functionBasicBlock = &functionBasicBlock;
    return this->functionBasicBlock;
}

const BasicBlock* FunctionOperation::getFunctionBasicBlock() const { return functionBasicBlock; }
BasicBlock* FunctionOperation::getFunctionBasicBlock() { return functionBasicBlock; }
const std::vector<PrimitiveStamp>& FunctionOperation::getInputArgs() const { return inputArgs; }
Types::StampPtr FunctionOperation::getOutputArg() const { return getStamp(); }

std::string FunctionOperation::toString() const {
    return fmt::format("{}({})", name, fmt::join(inputArgNames.begin(), inputArgNames.end(), ","));
}
bool FunctionOperation::classof(const Operation* Op) { return Op->getOperationType() == OperationType::FunctionOp; }
const std::vector<std::string>& FunctionOperation::getInputArgNames() const { return inputArgNames; }

}// namespace NES::Nautilus::IR::Operations
