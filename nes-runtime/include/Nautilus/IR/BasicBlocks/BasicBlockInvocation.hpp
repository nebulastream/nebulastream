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
    BasicBlockPtr getBlock() const;
    void addArgument(OperationPtr argument);
    void removeArgument(uint64_t argumentIndex);

    /**
     * @brief Iterate over args, find arg that matches OperationPtr and return index.
     * 
     * @return int: arg index for provided OperationPtr. Is -1 if no arg that matches OperationPtr was found.
     */
    int getOperationArgIndex(Operations::OperationPtr);

    /**
     * @brief Get all arguments of next block invocation, meaning all args of the branching operation (br, if, loop,..)
     * 
     * @return std::vector<OperationPtr> : vector with all arguments of the next block invocation.
     */
    std::vector<OperationPtr> getArguments() const;
    std::string toString() override;

  private:
    BasicBlockPtr basicBlock;
    std::vector<OperationWPtr> operations;
};

}// namespace NES::Nautilus::IR::Operations
#endif  // NES_RUNTIME_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCKINVOCATION_HPP_
