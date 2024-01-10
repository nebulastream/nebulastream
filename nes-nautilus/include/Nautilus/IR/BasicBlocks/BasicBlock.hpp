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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_

#include <Nautilus/IR/BasicBlocks/BasicBlockArgument.hpp>
#include <Nautilus/IR/Operations/Operation.hpp>
#include <memory>
#include <vector>

namespace NES::Nautilus::IR {

class BasicBlock : public std::enable_shared_from_this<BasicBlock> {
  public:
    /**
     * @brief BasicBlock used for control flow in NES IR
     * @param Operations: A list of Operations that are executed in the BasicBlock.
     * @param nextBlocks : The BasicBlock that is next in the control flow of the execution.
     */
    explicit BasicBlock(std::string identifier,
                        int32_t scopeLevel,
                        std::vector<Operations::OperationPtr> operations,
                        std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments);
    virtual ~BasicBlock() = default;
    [[nodiscard]] std::string getIdentifier();
    void setIdentifier(const std::string& identifier);
    [[nodiscard]] uint32_t getScopeLevel();
    void setScopeLevel(uint32_t scopeLevel);

    /**
     * @brief Get the number of edges that lead back from the loop body to the loop header.
     */
    [[nodiscard]] uint32_t getNumLoopBackEdges();

    /**
     * @brief Increment counter for edges that lead back from the loop body to the loop header.
     */
    void incrementNumLoopBackEdge();

    /**
     * @brief Check if the counter for edges that lead back from the loop body to the loop header is > 0.
     */
    [[nodiscard]] bool isLoopHeaderBlock();
    [[nodiscard]] std::vector<Operations::OperationPtr> getOperations();
    [[nodiscard]] Operations::OperationPtr getOperationAt(size_t index);
    [[nodiscard]] Operations::OperationPtr getTerminatorOp();
    [[nodiscard]] std::vector<std::shared_ptr<Operations::BasicBlockArgument>> getArguments();

    // NESIR Assembly
    std::shared_ptr<BasicBlock> addOperation(Operations::OperationPtr operation);
    std::shared_ptr<BasicBlock> addLoopHeadBlock(std::shared_ptr<BasicBlock> loopHeadBlock);
    std::shared_ptr<BasicBlock> addNextBlock(std::shared_ptr<BasicBlock> nextBlock);
    void addNextBlock(std::shared_ptr<BasicBlock> nextBlock, std::vector<Operations::OperationPtr> inputArguments);
    std::shared_ptr<BasicBlock> addTrueBlock(std::shared_ptr<BasicBlock> thenBlock);
    std::shared_ptr<BasicBlock> addFalseBlock(std::shared_ptr<BasicBlock> elseBlock);
    void removeOperation(Operations::OperationPtr operation);
    void addOperationBefore(Operations::OperationPtr before, Operations::OperationPtr operation);
    void addPredecessor(std::shared_ptr<BasicBlock> predecessor);
    std::vector<std::weak_ptr<BasicBlock>>& getPredecessors();
    uint64_t getIndexOfArgument(std::shared_ptr<Operations::Operation> arg);
    // void popOperation();
    void replaceTerminatorOperation(Operations::OperationPtr newTerminatorOperation);
    [[nodiscard]] std::pair<std::shared_ptr<BasicBlock>, std::shared_ptr<BasicBlock>> getNextBlocks();

  private:
    std::string identifier;
    uint32_t scopeLevel;
    uint32_t numLoopBackEdges;
    std::vector<Operations::OperationPtr> operations;
    std::vector<std::shared_ptr<Operations::BasicBlockArgument>> arguments;
    std::vector<std::weak_ptr<BasicBlock>> predecessors;
};
using BasicBlockPtr = std::shared_ptr<BasicBlock>;

}// namespace NES::Nautilus::IR
#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_IR_BASICBLOCKS_BASICBLOCK_HPP_
