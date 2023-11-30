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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BRANCHOPERATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BRANCHOPERATION_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/BasicBlocks/BasicBlockInvocation.hpp>

namespace NES::Nautilus::IR::Operations {
/**
 * @brief Terminator Operation(Op), must be last Op in BasicBlock(BB). Passes control flow from one BB to another.
 * 
 */
class BranchOperation : public Operation {
  public:
    explicit BranchOperation();
    ~BranchOperation() override = default;

    BasicBlockInvocation& getNextBlockInvocation();
    std::string toString() override;
    static bool classof(const Operation* Op);

  private:
    BasicBlockInvocation basicBlock;
};
}// namespace NES::Nautilus::IR::Operations
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_OPERATIONS_BRANCHOPERATION_HPP_
