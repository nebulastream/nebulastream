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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_FUNCTIONOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_FUNCTIONOPERATION_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>

namespace NES::Nautilus::IR::Operations {
class FunctionOperation : public Operation {
  public:
    explicit FunctionOperation(std::string name,
                               std::vector<PrimitiveStamp> inputArgs,
                               std::vector<std::string> inputArgNames,
                               Types::StampPtr outputArg);
    ~FunctionOperation() override = default;

    [[nodiscard]] const std::string& getName() const;
    BasicBlockPtr addFunctionBasicBlock(BasicBlockPtr functionBasicBlock);
    BasicBlockPtr getFunctionBasicBlock();
    [[nodiscard]] const std::vector<PrimitiveStamp>& getInputArgs() const;
    [[nodiscard]] Types::StampPtr getOutputArg() const;
    [[nodiscard]] const std::vector<std::string>& getInputArgNames() const;

    std::string toString() override;
    static bool classof(const Operation* Op);

  private:
    std::string name;
    BasicBlockPtr functionBasicBlock;
    std::vector<PrimitiveStamp> inputArgs;
    std::vector<std::string> inputArgNames;
};
}// namespace NES::Nautilus::IR::Operations

#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_FUNCTIONOPERATION_HPP_
