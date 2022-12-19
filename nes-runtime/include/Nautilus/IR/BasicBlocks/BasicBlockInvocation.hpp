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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
#include <Nautilus/IR/BasicBlocks/BasicBlock.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
namespace NES::Nautilus::IR::Operations {

class BasicBlockInvocation : public Operation {
  public:
    BasicBlockInvocation();
    void setBlock(BasicBlockPtr block);
    BasicBlockPtr getNextBlock();
    void addArgument(OperationPtr argument);
    void setBranchOpAt(size_t index, Operations::OperationPtr operation);
    void clearBranchOps();
    void removeArgument(uint64_t argumentIndex); //Todo remove
    int getOperationArgIndex(Operations::OperationPtr);
    std::vector<OperationPtr> getBranchOps();
    std::string toString() override;

  private:
    BasicBlockPtr basicBlock;
    // changed because StructuredControlFlowPhaseTest 9 with value scoping lead to weak pointer returning null pointer 
    // even though the underlying pointer of the shared pointer was still alive
    std::vector<OperationPtr> branchOps;
};

}// namespace NES::Nautilus::IR::Operations
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
